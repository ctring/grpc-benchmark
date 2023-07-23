import argparse
import json
import os
import subprocess

import pandas as pd

from pprint import pprint


def main(args):
    binary = find_binary()
    if binary is not None:
        print(f"Using binary: {binary}")
    else:
        print("No binary for the benchmark found")
        exit(1)
    results = []
    for nclient in args.nclients:
        for ncmd in args.ncmds:
            cmd = [
                "target/release/client",
                "--clients", str(nclient),
                "--txns", str(args.ntxn),
                "--commands", str(ncmd),
                "--json"
            ]
            output = subprocess.check_output(cmd)
            record = json.loads(output)
            record.update({"nclient": nclient, "ncmd": ncmd, "nop": 1})
            results.append(record)
            pprint(record)

    df = pd.DataFrame(results)
    df.to_csv(args.output, index=False)


def find_binary():
    if os.path.exists("target/release/client"):
        return "target/release/client"
    elif os.path.exists("../target/release/client"):
        return "../target/release/client"
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--nclients",
        nargs="+",
        type=int,
        default=[1, 2, 4, 8, 16],
        help="Number of clients",
    )
    parser.add_argument("--ntxn", default=1000, help="Number of transactions")
    parser.add_argument(
        "--ncmds",
        nargs="+",
        type=int,
        default=[2, 3, 6, 11, 16, 21],
        help="Number of commands per transaction",
    )
    parser.add_argument("--output", default="results.csv", help="Output file")
    args = parser.parse_args()

    main(args)
