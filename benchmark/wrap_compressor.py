#!/usr/bin/env python3

import argparse
import os
import subprocess
import time

parser = argparse.ArgumentParser(description='compressor execution wrapper')
parser.add_argument("compressor")
parser.add_argument("ext")
parser.add_argument("filename")

args = parser.parse_args()

cmdline = [args.compressor, "-9", "-f", "-c", args.filename]

t0 = time.time()

outfilename = ".".join([args.filename, args.ext])
with open(outfilename, "wb") as f:
    p = subprocess.Popen(cmdline, stdout=f)
    p.wait()
    f.flush()

dt = round(1000.0 * (time.time() - t0))

n = os.path.getsize(args.filename)
nout = os.path.getsize(outfilename)

print(f"RESULT algo={args.compressor} n={str(n)} nout={str(nout)} time={str(dt)}")
