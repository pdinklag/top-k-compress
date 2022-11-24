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

cmdline = [args.compressor, "-9", "-kf", args.filename]

t0 = time.time()

p = subprocess.Popen(cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
pout, perr = p.communicate()

dt = time.time() - t0

outfilename = ".".join([args.filename, args.ext])

n = os.path.getsize(args.filename)
nout = os.path.getsize(outfilename)

print(f"RESULT algo={args.compressor} n={str(n)} nout={str(nout)} time={str(dt)}")
