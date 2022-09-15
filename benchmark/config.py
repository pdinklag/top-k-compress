gzip_bin = "/usr/bin/gzip"
bzip2_bin = "/usr/bin/bzip2"
topk_bin = "/scratch/top-k-compress/build/src/comp"

ks      = ["256K", "512K", "1M", "2M", "4M"]
windows = [8, 12, 16, 24, 32]
sel_add_windows = [48, 64, 96, 128]

options = ["-c", "8M", "-s", "2K"]

files = [
    "/scratch/data/cere.200Mi",
    "/scratch/data/english.200Mi",
    "/scratch/data/dblp.xml.200Mi",
    "/scratch/data/dna.200Mi",
    "/scratch/data/einstein.en.txt.200Mi",
    "/scratch/data/proteins.200Mi",
    "/scratch/data/random64K.200Mi",
    "/scratch/data/sources.200Mi",
]
