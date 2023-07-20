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
        super().__init__("gzip", "gz", filename, 240)

class Bzip2Task(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("bzip2", "bz2", filename, 30)

class XzTask(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("xz", "xz", filename, 300)

class ProjectTask(Task):
    def __init__(self, compressor: str, xcmd: list[str], filename: str, max_time: int):
        global project_path

        bin = os.path.join(project_path, "build", "src", compressor)
        super().__init__([bin] + xcmd + [filename], max_time)

# class Lz77LpfTask(ProjectTask):
#     def __init__(self, filename: str):
#         super().__init__("lz77-lpf", ["-t", "4"], filename, 60)

# class GzipTdcTask(ProjectTask):
#     def __init__(self, filename: str):
#         super().__init__("gzip-tdc", [], filename, 120)

# class Lz78Task(ProjectTask):
#     def __init__(self, filename: str):
#         super().__init__("lz78", [], filename, 120)

# class LzEndTask(ProjectTask):
#     def __init__(self, filename: str):
#         super().__init__("lzend", [], filename, 600)

# class LzEndKKTask(ProjectTask):
#     def __init__(self, filename: str, w: str):
#         super().__init__("lzend-kk", [f"--window={w}"], filename, 1200)

class LzEndBlockwiseTask(ProjectTask):
    def __init__(self, filename: str, w: str):
        super().__init__("lzend-blockwise", [f"--window={w}"], filename, 180)

class TopkTask(ProjectTask):
    def __init__(self, compressor: str, w: str, k: str, xcmd: list[str], filename: str, max_time: int):
        super().__init__(compressor, [f"--window={w}", "-k", k, "-c", k] + xcmd, filename, max_time)

class TopkLz78Task(TopkTask):
    def __init__(self, filename: str, k: str):
        super().__init__("topk-lz78", str(0), k, [], filename, 30)

# class TopkLzEndTask(TopkTask):
#     def __init__(self, filename: str, w: str, k: str):
#         super().__init__("topk-lzend", w, k, [], filename, 400)

# class TopkLz77Task(TopkTask):
#     def __init__(self, filename: str, w: int, k: str):
#         super().__init__("topk-lz77", str(w), k, ["-t", "4"], filename, 600)

# class TopkLz77FastTask(TopkTask):
#     def __init__(self, filename: str, w: int, k: str):
#         super().__init__("topk-lz77-fast", str(w), k, ["-t", "4"], filename, 60)

class TopkSelTask(TopkTask):
    def __init__(self, filename: str, w: int, k: str):
        super().__init__("topk-sel", str(w), k, [], filename, 180)

# class TopkSampleTask(ProjectTask):
#     def __init__(self, filename: str, s: int, mn: int, mx: int, k: str):
#         super().__init__("topk-sample", ["-k", k, f"--sample={s}", f"--min={mn}", f"--max={mx}"], filename, 120)

def create_job():
    global cp_task

    job = LidoLongStd01Job()
    #job = LidoMedStd01Job()
    #job = LidoShortStd01Job()
    job.add(cp_task)
    return job

shebang = "#!/usr/bin/bash"
master = [shebang]

for file in config.files:
    name = os.path.basename(file)
    size = os.path.getsize(file)

    workfile = os.path.join(config.workdir, name)
    cp_task = CopyFileTask(file, workfile)

    tasks = [
        GzipTask(workfile),
        Bzip2Task(workfile),
        XzTask(workfile),
    ]

    for k in config.ks:
        #tasks.append(TopkSampleTask(workfile, 4, 10, 20, k))
        tasks.append(TopkLz78Task(workfile, k))
        for w in config.windows:
            tasks.append(TopkSelTask(workfile, w, k))

    for w in ['128Mi', '256Mi', '512Mi']:
        tasks.append(LzEndBlockwiseTask(workfile, w))

    ref_size = 200 * 1024 * 1024;
    time_scale = float(size) / float(ref_size);

    print(f"time_scale={time_scale}")
    cp_task.max_time = int(round(time_scale * float(cp_task.max_time)))
    for task in tasks:
        print(f"scaling time of task {str(task)} from {task.max_time} to {round(time_scale * task.max_time)}")
        task.max_time = int(round(time_scale * float(task.max_time)))

    number = 1
    for job in generate(tasks, create_job):
        jobname = f"{name}_{number:02d}"
        jobfile = f"job-{jobname}.sh"
        with open(os.path.join(args.outdir, jobfile), "w") as f:
            f.write("\n".join([shebang] + job.script()))

        master.append("echo " + job.cmd(os.path.abspath(jobfile)))
        master.append(job.cmd(os.path.abspath(jobfile)))
        number += 1

with open(os.path.join(args.outdir, "benchmark.sh"), "w") as f:
    f.write("\n".join(master))

print(f"Benchmark scripts written to {args.outdir}")
