# top-k-compress

This repository contains the source code to accompany the paper *Top-k frequent patterns in streams and parameterized-space LZ compression*, which is currently under review.

More information as how to build and execute the implemented compressors is to follow.

## Results

The `txt` files in the `results` directory contain the raw experimental results that are plotted in the paper.

In case you decide to have a look, please note the following:

* For every input file and set of parameters, there are three lines, each corresponding to a different iteration. In the paper, running times are given as averages.
* The `block_size` column has nothing to do with the blockwise LZ77 compression. Rather, it contains the number of phrases in an encoded block (each of which has its own Huffman table). The block size for blockwise LZ compression is contained in the `window` column.