pub mod pb {
    tonic::include_proto!("grpc.examples.echo");
}

use clap::Parser;
use histogram::Histogram;
use pb::{echo_client::EchoClient, EchoRequest};
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request};

async fn unary_streaming_echo(
    client: &mut EchoClient<Channel>,
    num: usize,
    cmd_latency: &Histogram,
) {
    for i in 0..num {
        let request = tonic::Request::new(EchoRequest {
            message: format!("msg {:02}", i),
        });
        let cmd_time = Instant::now();
        client.unary_echo(request).await.unwrap();
        cmd_latency
            .increment(cmd_time.elapsed().as_nanos() as u64, 1)
            .unwrap();
    }
}

async fn bidirectional_streaming_echo(client: &mut EchoClient<Channel>, num: usize) {
    let (tx, mut rx) = mpsc::channel(128);

    let outbound = async_stream::stream! {
        for i in 0..num {
            yield EchoRequest {
                message: format!("msg {:02}", i),
            };
            if i == num - 1 {
                break;
            }
            rx.recv().await.unwrap();
        }
    };

    let response = client
        .bidirectional_streaming_echo(Request::new(outbound))
        .await
        .unwrap();

    let mut resp_stream = response.into_inner();

    while let Some(received) = resp_stream.next().await {
        received.unwrap();
        let _ = tx.try_send(true);
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, short, default_value = "10", help = "Number of clients.")]
    clients: i32,

    #[arg(long, default_value = "10", help = "Number of transactions.")]
    txns: usize,

    #[arg(long, default_value = "5", help = "Number of commands per txn.")]
    commands: usize,

    #[arg(long, default_value = "false", help = "Use streaming")]
    stream: bool,

    #[arg(long, default_value = "false", help = "Simple output")]
    json: bool,
}

#[tokio::main]
#[allow(unused_must_use)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut tasks = Vec::new();
    for _ in 0..cli.clients {
        let task = tokio::spawn(async move {
            let mut client = EchoClient::connect("http://[::1]:50051").await.unwrap();
            let txn_latency = Histogram::builder().build().unwrap();
            let cmd_latency = Histogram::builder().build().unwrap();

            let workload_time = Instant::now();
            for _ in 0..cli.txns {
                let txn_time = Instant::now();
                if cli.stream {
                    bidirectional_streaming_echo(&mut client, cli.commands).await;
                } else {
                    unary_streaming_echo(&mut client, cli.commands, &cmd_latency).await;
                };
                txn_latency.increment(txn_time.elapsed().as_nanos() as u64, 1);
            }
            let elapsed = workload_time.elapsed();
            BenchmarkResult {
                tps: cli.txns as f64 / elapsed.as_secs_f64(),
                txn_latency,
                cmd_latency,
                elapsed,
                committed_txns: cli.txns as u32,
            }
        });
        tasks.push(task);
    }

    let mut final_result = BenchmarkResult::default();
    for task in tasks {
        let task_result = tokio::join!(task).0.unwrap();
        final_result.merge(task_result);
    }

    final_result.show(cli.json);
    Ok(())
}

pub struct BenchmarkResult {
    pub tps: f64,
    pub txn_latency: Histogram,
    pub cmd_latency: Histogram,
    pub elapsed: Duration,
    pub committed_txns: u32,
}

impl Default for BenchmarkResult {
    fn default() -> Self {
        Self {
            tps: 0.0,
            txn_latency: Histogram::builder().build().unwrap(),
            cmd_latency: Histogram::builder().build().unwrap(),
            elapsed: Duration::default(),
            committed_txns: 0,
        }
    }
}

impl BenchmarkResult {
    pub fn merge(&mut self, other: Self) {
        self.tps += other.tps;
        self.txn_latency.merge(&other.txn_latency).unwrap();
        self.cmd_latency.merge(&other.cmd_latency).unwrap();
        self.elapsed = self.elapsed.max(other.elapsed);
        self.committed_txns += other.committed_txns;
    }

    pub fn show(&self, json: bool) {
        let output = json!({
            "elapsed": self.elapsed.as_secs_f64(),
            "committed": self.committed_txns,
            "tps": self.tps,
            "txn_lat_min": percentile_ms(&self.txn_latency, 0.0),
            "txn_lat_p50": percentile_ms(&self.txn_latency, 50.0),
            "txn_lat_p90": percentile_ms(&self.txn_latency, 90.0),
            "txn_lat_p99": percentile_ms(&self.txn_latency, 99.0),
            "txn_lat_max": percentile_ms(&self.txn_latency, 100.0),
            "cmd_lat_min": percentile_ms(&self.cmd_latency, 0.0),
            "cmd_lat_p50": percentile_ms(&self.cmd_latency, 50.0),
            "cmd_lat_p90": percentile_ms(&self.cmd_latency, 90.0),
            "cmd_lat_p99": percentile_ms(&self.cmd_latency, 99.0),
            "cmd_lat_max": percentile_ms(&self.cmd_latency, 100.0),
        });

        if json {
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
            return;
        }

        println!("Elapsed: {:.3} s", output["elapsed"].as_f64().unwrap());
        println!(
            "Committed Transactions: {}",
            output["committed"].as_i64().unwrap()
        );
        println!("Throughput: {:.0} txn/s", output["tps"].as_f64().unwrap());
        println!("Transaction Latency:");
        println!("\tmin: {:.3} ms", output["txn_lat_min"].as_f64().unwrap());
        println!("\tp50: {:.3} ms", output["txn_lat_p50"].as_f64().unwrap());
        println!("\tp90: {:.3} ms", output["txn_lat_p90"].as_f64().unwrap());
        println!("\tp99: {:.3} ms", output["txn_lat_p99"].as_f64().unwrap());
        println!("\tmax: {:.3} ms", output["txn_lat_max"].as_f64().unwrap());
        println!("Command Latency:");
        println!("\tmin: {:.3} ms", output["cmd_lat_min"].as_f64().unwrap());
        println!("\tp50: {:.3} ms", output["cmd_lat_p50"].as_f64().unwrap());
        println!("\tp90: {:.3} ms", output["cmd_lat_p90"].as_f64().unwrap());
        println!("\tp99: {:.3} ms", output["cmd_lat_p99"].as_f64().unwrap());
        println!("\tmax: {:.3} ms", output["cmd_lat_max"].as_f64().unwrap());
    }
}

fn percentile_ms(hist: &Histogram, p: f64) -> f64 {
    let bucket = hist.percentile(p).unwrap();
    let mid = (bucket.high() + bucket.low()) / 2;
    Duration::from_nanos(mid).as_secs_f64() * 1000.0
}
