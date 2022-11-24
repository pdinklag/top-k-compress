#!/usr/bin/env python3

from jobs import *

import os

project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))

class WrapCompressorTask(Task):
    def __init__(self, compressor: str, ext: str, filename: str, max_time: int):
        global project_path

        wrap_cmd = os.path.join(project_path, "benchmark", "wrap_compressor.py")
        super().__init__([wrap_cmd, compressor, ext, filename], max_time)

class GzipTask(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("gzip", "gz", filename, 60)

class Bzip2Task(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("bzip2", "bz2", filename, 60)

class XzTask(WrapCompressorTask):
    def __init__(self, filename: str):
        super().__init__("xz", "xz", filename, 360)

class TopkTask(Task):
    def __init__(self, compressor: str, w: int, k: str, filename: str, max_time: int):
        global project_path

        bin = os.path.join(project_path, "build", "src", compressor)
        super().__init__([bin, "-w", str(w), "-k", k, "-c", "8M", "-s", "2K", filename], max_time)

class TopkLz78Task(TopkTask):
    def __init__(self, filename: str, k: str, max_time: int):
        super().__init__("topk-lz78", 0, k, filename, max_time)

class TopkLz77Task(TopkTask):
    def __init__(self, filename: str, w: int, k: str, max_time: int):
        super().__init__("topk-lz77", w, k, filename, max_time)

class TopkLz77FastTask(TopkTask):
    def __init__(self, filename: str, w: int, k: str, max_time: int):
        super().__init__("topk-lz77-fast", w, k, filename, max_time)

class TopkSelTask(TopkTask):
    def __init__(self, filename: str, w: int, k: str, max_time: int):
        super().__init__("topk-lz77-sel", w, k, filename, max_time)

tasks = []
