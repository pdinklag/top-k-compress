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
    logf.flush()

    # print brief result to stdout
    print(f"{brief}\t{n}\t{ncomp}\t{dt}")
    sys.stdout.flush()

def gzip(filename):
    bench(
        [config.gzip_bin, "-9", "-k", filename],
        "gzip",
        filename,
        ".gz")

def bzip2(filename):
    bench(
        [config.bzip2_bin, "-9", "-k", filename],
        "bzip2",
        filename,
        ".bz2")

def topk_exh(filename, k, w):
    bench(
        [config.topk_bin, "-k", str(k), "-w", str(w), filename] + config.options + [filename],
        f"topk-exh-k{k}_w{w}",
        filename,
        ".exh")

def topk_exh_huff(filename, k, w):
    bench(
        [config.topk_bin, "-k", str(k), "-w", str(w), "--huff"] + config.options + [filename],
        f"topk-exh-huff-k{k}_w{w}",
        filename,
        ".exhh")

def topk_sel(filename, k, w):
    bench(
        [config.topk_bin, "-x", "-k", str(k), "-w", str(w), filename] + config.options + [filename],
        f"topk-sel-k{k}_w{w}",
        filename,
        ".sel")

def topk_sel_huff(filename, k, w):
    bench(
        [config.topk_bin, "-x", "-k", str(k), "-w", str(w), "--huff"] + config.options + [filename],
        f"topk-sel-huff-k{k}_w{w}",
        filename,
        ".selh")

def topk_lz78(filename, k, w):
    bench(
        [config.topk_bin, "-z", "-k", str(k), "-w", str(w), filename] + config.options + [filename],
        f"topk-lz78-k{k}_w{w}",
        filename,
        ".lz78")

def topk_lz78_huff(filename, k, w):
    bench(
        [config.topk_bin, "-z", "-k", str(k), "-w", str(w), "--huff"] + config.options + [filename],
        f"topk-lz78-huff-k{k}_w{w}",
        filename,
        ".lz78h")

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

        #gzip(filename)
        #bzip2(filename)
        for w in config.windows:
            for k in config.ks:
                pass
                #topk_exh(filename, k, w)
                #topk_exh_huff(filename, k, w)
                
        for w in config.windows:
            for k in config.ks:
                pass
                #topk_lz78(filename, k, w)
                #topk_lz78_huff(filename, k, w)
                
        for w in config.windows + config.sel_add_windows:
            for k in config.ks:
                topk_sel(filename, k, w)
                topk_sel_huff(filename, k, w)
        
        print()
