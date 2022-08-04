#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import time

import config

def bench(cmd, brief, filename, ext):
    #global logf

    # run command
    t0 = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pout, perr = p.communicate()
    dt = time.time() - t0

    # get output stats
    n = os.path.getsize(filename)
    ncomp = os.path.getsize(filename + ext)
    ratio = float(ncomp) / float(n)

    # cleanup output file
    os.remove(filename + ext)

    # print stats to logfile
    logf.write("\n".join([
        "------------------------------------------",
        " ".join(cmd),
        pout.decode("utf-8"),
        f"time:  {dt}s",
        f"in:    {n}",
        f"out:   {ncomp}",
        f"ratio: {ratio}"
    ]))
    logf.write("\n\n")

    # print brief result to stdout
    print(f"{brief}\t{n}\t{ncomp}\t{dt}")
    sys.stdout.flush()

def gzip9(filename):
    bench(
        [config.gzip_bin, "-9", "-k", filename],
        "gzip-9",
        filename,
        ".gz")

def topk(filename, k, w):
    bench(
        [config.topk_bin, "-k", str(k), "-c", str(k), "-w", str(w), filename],
        f"topk-k{k}_w{w}",
        filename,
        ".topk")

def topkh(filename, k, w):
    bench(
        [config.topkh_bin, "-k", str(k), "-c", str(k), "-w", str(w), "--huff", filename],
        f"topk-k{k}_w{w}-huff",
        filename,
        ".topkh")

# parse args
parser = argparse.ArgumentParser(description='top-k compression benchmark')
parser.add_argument("--log", type=str, default="bench.log", help="Output file for detailed logs")
args = parser.parse_args()

# open logfile
with open(args.log, "w") as logf:
    # run
    for filename in config.files:
        print(f"Benchmarking for {filename} ...")
        print()
        print("algo\tn\tn'\ttime")
        sys.stdout.flush()

        gzip9(filename)
        for w in config.windows:
            for k in config.ks:
                topk(filename, k, w)
                topkh(filename, k, w)
        print()
