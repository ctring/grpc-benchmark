import subprocess
import pandas as pd

TXNS = 1000
TRIALS = 3

results = []
for stream in [False, True]:
    for clients in [1, 2, 4, 8, 16, 32, 64]:
        for commands in [1, 2, 5, 10, 15, 20, 25, 30]:
            cmd = [
                "target/release/client",
                "--clients", str(clients),
                "--txns", str(TXNS),
                "--commands", str(commands),
                "--simple"
            ]
            if stream:
                cmd.append("--stream")
            elapsed_ms = 0
            for trials in range(TRIALS):
                output = subprocess.check_output(cmd)
                elapsed_ms += float(output.strip()) / 1000
            elapsed_ms /= TRIALS
            record = {
                "stream": stream,
                "clients": clients,
                "commands": commands,
                "elapsed_ms": elapsed_ms,
                "tps": TXNS * clients / (elapsed_ms / 1000),
                "command_ms": elapsed_ms / (commands * TXNS),
                "txn_ms": elapsed_ms / TXNS
            }
            results.append(record)
            print(record)

df = pd.DataFrame(results)
df.to_csv("results.csv", index=False)
