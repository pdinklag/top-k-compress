#!/usr/bin/env python3

import argparse
import os

from jobs import *
from slurm import *

import config

parser = argparse.ArgumentParser(description='top-k compression benchmark generator')
parser.add_argument("outdir")
args = parser.parse_args()

project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))

class WrapCompressorTask(Task):
    def __init__(self, compressor: str, ext: str, filename: str, max_time: int):
        global project_path

        wrap_cmd = os.path.join(project_path, "benchmark", "wrap_compressor.py")
        super().__init__([wrap_cmd, compressor, ext, filename], max_time)

class GzipTask(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("gzip", "gz", filename, 120)

class Bzip2Task(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("bzip2", "bz2", filename, 120)

class XzTask(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("xz", "xz", filename, 360)

class ProjectTask(Task):
    def __init__(self, compressor: str, xcmd: list[str], filename: str, max_time: int):
        global project_path

        bin = os.path.join(project_path, "build", "src", compressor)
        super().__init__([bin] + xcmd + [filename], max_time)

class Lz77LpfTask(ProjectTask):
    def __init__(self, filename: str):
        super().__init__("lz77-lpf", ["-t", "4"], filename, 60)

class GzipTdcTask(ProjectTask):
    def __init__(self, filename: str):
        super().__init__("gzip-tdc", [], filename, 120)

class TopkTask(ProjectTask):
    def __init__(self, compressor: str, w: int, k: str, xcmd: list[str], filename: str, max_time: int):
        super().__init__(compressor, [f"--window={str(w)}", "-k", k, "-c", "8M", "-s", "2K"] + xcmd, filename, max_time)

class TopkLz78Task(TopkTask):
    def __init__(self, filename: str, k: str):
        super().__init__("topk-lz78", 0, k, [], filename, 60)

class TopkLz77Task(TopkTask):
    def __init__(self, filename: str, w: int, k: str):
        super().__init__("topk-lz77", w, k, ["-t", "4"], filename, 600)

class TopkLz77FastTask(TopkTask):
    def __init__(self, filename: str, w: int, k: str):
        super().__init__("topk-lz77-fast", w, k, ["-t", "4"], filename, 60)

class TopkSelTask(TopkTask):
    def __init__(self, filename: str, w: int, k: str):
        super().__init__("topk-sel", w, k, [], filename, 480)

def create_job():
    global cp_task

    job = LidoMedStd01Job()
    job.add(cp_task)
    return job

shebang = "#!/usr/bin/bash"
master = [shebang]

for file in config.files:
    name = os.path.basename(file)
    workfile = os.path.join(config.workdir, name)
    cp_task = CopyFileTask(file, workfile)

    tasks = [
        GzipTask(workfile),
        Bzip2Task(workfile),
        XzTask(workfile),
        GzipTdcTask(workfile),
        Lz77LpfTask(workfile)
    ]

    for k in config.ks:
        tasks.append(TopkLz78Task(workfile, k))
        for w in config.windows:
            tasks.append(TopkLz77FastTask(workfile, w, k))
        for w in config.windows:
            tasks.append(TopkLz77Task(workfile, w, k))
        for w in config.windows:
            tasks.append(TopkSelTask(workfile, w, k))

    number = 1
    for job in generate(tasks, create_job):
        jobname = f"{name}_{number:02d}"
        jobfile = f"job-{jobname}.sh"
        with open(os.path.join(args.outdir, jobfile), "w") as f:
            f.write("\n".join([shebang] + job.script()))

        master.append(job.cmd(os.path.abspath(jobfile)))
        number += 1

with open(os.path.join(args.outdir, "benchmark.sh"), "w") as f:
    f.write("\n".join(master))

print(f"Benchmark scripts written to {args.outdir}")
