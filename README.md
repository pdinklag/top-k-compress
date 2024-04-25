# top-k-compress

This repository contains the source code to accompany the SEA 2024 paper *Top-k frequent patterns in streams and parameterized-space LZ compression*.

## Building

The source code is written in C++20, a compiler with the corresponding set of features is required (we used GCC 13.2).

The project is best built using CMake. After cloning the repository, switch to the repository's root and execute the following:

```sh
mkdir build; cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```

After a successful build, the `build/src` directory will contain the binaries necessary to conduct the [Experiments](#experiments).

## Experiments

After [building](#building) the binaries, the experiments shown in the paper were conducted by running the relevant binaries as follows:

| Algorithm      | Command-line                                        |
| -------------- | --------------------------------------------------- |
| topk-lz78      | `build/src/topk-lz78 -k <k> <FILE>`                 |
| topk-lz77      | `build/src/topk-lz77 -k <k> -w <block size> <FILE>` |
| blockwise-lz77 | `build/src/lz77-blockwise -w <block size> <FILE>`   |

The `-k` and `-w` options support IEC units. For example, you may state `1Mi` to say "one mebi" or 2<sup>10</sup>.

The compressed files will be named `<FILE>.<ext>`, where `<ext>` is an extension depending on the used algorithm. The original file is retained.

In the console output, note that the `block_size` column has nothing to do with the blockwise LZ77 compression. Rather, it contains the number of phrases in an encoded block (each of which has its own Huffman table). The block size for blockwise LZ compression (also used in *topk-lz77*) is contained in the `window` column.

### Decoding

Compressed files can be decoded by running the respective binary with the `-d` parameter (e.g., `build/src/topk-lz78 -d <FILE>.topklz78`). The output will be written to `<FILE>.<ext>.dec`. Using a checksum utility of your choice, you can then verify that it is equal to the original `<FILE>`.

### Competitors

The competitors in the experiments were [gzip](https://www.gzip.org/), [xz](https://tukaani.org/xz/), [zstd](http://facebook.github.io/zstd/), [bzip2](https://sourceware.org/bzip2/) and [bsc](http://libbsc.com/).

### Results

The `txt` files in the `results` directory contain the raw experimental results that are plotted in the paper.

For every input file and set of parameters, there are three lines, each corresponding to a different iteration. In the paper, running times are given as averages.

## License

The source code is published under the [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license (just like the paper).

