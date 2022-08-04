gzip_bin = "/usr/bin/gzip"
topk_bin = "/scratch/top-k-compress/build/src/comp"
topkh_bin = "/scratch/top-k-compress/build/src/comp"

ks      = ["1M", "2M", "4M", "8M", "16M"]
windows = [8, 12, 16, 20, 24, 32]

sketch_columns = "10M"

files = [
    "/scratch/data/cere.100Mi",
    "/scratch/data/english.100Mi",
    "/scratch/data/dblp.xml.100Mi",
    "/scratch/data/dna.100Mi",
    "/scratch/data/einstein.de.txt.100Mi",
    "/scratch/data/proteins.100Mi",
    "/scratch/data/random64K.100Mi",
    "/scratch/data/sources.100Mi",
]
