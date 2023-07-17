pub mod pb {
    tonic::include_proto!("grpc.examples.echo");
}

use clap::Parser;
use std::time::Instant;
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request};

use pb::{echo_client::EchoClient, EchoRequest};

async fn unary_streaming_echo(client: &mut EchoClient<Channel>, num: usize) {
    for i in 0..num {
        let request = tonic::Request::new(EchoRequest {
            message: format!("msg {:02}", i),
        });

        client.unary_echo(request).await.unwrap();
    }
}

async fn bidirectional_streaming_echo(client: &mut EchoClient<Channel>, num: usize) {
    let outbound = async_stream::stream! {
        for i in 0..num {
            yield EchoRequest {
                message: format!("msg {:02}", i),
            };
        }
    };

    let response = client
        .bidirectional_streaming_echo(Request::new(outbound))
        .await
        .unwrap();

    let mut resp_stream = response.into_inner();

    while let Some(received) = resp_stream.next().await {
        received.unwrap();
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
    simple: bool,
}

#[tokio::main]
#[allow(unused_must_use)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut tasks = Vec::new();
    if !cli.simple {
        println!("Number of clients: {}", cli.clients);
        println!("Number of transactions: {}", cli.txns);
        println!("Number of commands per transaction: {}", cli.commands);
    }

    for _ in 0..cli.clients {
        let task = tokio::spawn(async move {
            let mut client = EchoClient::connect("http://[::1]:50051").await.unwrap();

            let start_time = Instant::now();
            for _ in 0..cli.txns {
                if cli.stream {
                    bidirectional_streaming_echo(&mut client, cli.commands).await;
                } else {
                    unary_streaming_echo(&mut client, cli.commands).await;
                }
            }
            let elapsed = start_time.elapsed().as_micros() as f32;
            elapsed
        });
        tasks.push(task);
    }

    let mut total_elapsed = 0.0;
    for task in tasks {
        total_elapsed += tokio::join!(task).0.unwrap();
    }

    let avg_elapsed = total_elapsed / cli.clients as f32;
    if cli.simple {
        println!("{}", avg_elapsed);
    } else {
        println!(
            "Elapsed time per client (avg): {:.2} ms",
            avg_elapsed / 1000.0
        );
        println!(
            "Latency per txn (avg): {:.2} ms",
            avg_elapsed / cli.txns as f32 / 1000.0
        );
        println!(
            "Latency per command (avg): {:.2} ms",
            avg_elapsed / (cli.txns as f32 * cli.commands as f32) / 1000.0
        );
        println!(
            "Txns per second: {:.2}",
            cli.txns as f32 / (avg_elapsed / 1000000.0)
        );
    }

    Ok(())
}
