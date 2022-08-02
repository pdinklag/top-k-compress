#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <iostream> // DEBUG

struct HuffmanCode {
    uint64_t word;   // the Huffman codeword, contained in the lowest _length_ bits in MSBF order
    uint8_t  length; // the length of the code, must be less than or equal to 64

    uint64_t bit_reverse() const {
        uint64_t w = word;
        uint64_t r = 0;
        for(size_t i = 0; i < length; i++) {
            r = (r << 1) | (w & 1);
            w >>= 1;
        }
        return r;
    }
};

// dynamic Huffman codes according to [Vitter, 1989]
template<std::unsigned_integral Letter>
class Vitter87 {
private:
    static constexpr bool debug_ = false;

    using NodeIndex = size_t;
    using BlockIndex = size_t;

    size_t n; // alphabet size
    size_t M; // # of letters with zero weight
    size_t E; // semantics?
    size_t R; // semantics?
    size_t Z; // semantics: root?

    // struct Node
    std::unique_ptr<Letter[]> alpha; // letter represented by node
    std::unique_ptr<BlockIndex[]> block; // block number of node

    // Letter -> leaf
    std::unique_ptr<NodeIndex[]> rep; // the node that represents a given letter

    // struct Block
    std::unique_ptr<size_t[]> weight; // weight of each node in block
    std::unique_ptr<NodeIndex[]> parent; // parent node of the leader of the block, if it exists, 0 otherwise
    std::unique_ptr<bool[]> parity; // 0 if the leader of a block is a left child or the root, 1 otherwise
    std::unique_ptr<NodeIndex[]> rtChild; // q if the block is a block if internal nodes and node q is the right child of the block's leader
    std::unique_ptr<NodeIndex[]> first; // leader of the block
    std::unique_ptr<NodeIndex[]> last; // smallest numbered node in block
    std::unique_ptr<BlockIndex[]> prevBlock; // linked list
    std::unique_ptr<BlockIndex[]> nextBlock; // linked list

    BlockIndex availBlock;

    bool is_leaf(NodeIndex const q) { return q <= n; }

    // return the node number of either the left or right child of node j, depending on parity
    NodeIndex find_child(NodeIndex const j, bool const parity) const {
        auto delta = 2 * (first[block[j]] - j) + 1 - parity;
        auto right = rtChild[block[j]];
        auto gap = right - last[block[right]];

        if(delta <= gap) {
            return right - delta;
        } else {
            delta = delta - gap - 1;
            right = first[prevBlock[block[right]]];
            gap = right - last[block[right]];
            if(delta <= gap) {
                return right - delta;
            } else {
                return first[prevBlock[block[right]]] - delta + gap + 1;
            }
        }
    }

    void interchange_leaves(NodeIndex const e1, NodeIndex const e2) {
        rep[alpha[e1]] = e2;
        rep[alpha[e2]] = e1;
        auto const temp = alpha[e1];
        alpha[e1] = alpha[e2];
        alpha[e2] = temp;
    }

    // subroutine of update
    std::pair<NodeIndex, NodeIndex> find_node(Letter const k) {
        if constexpr(debug_) std::cout << "find_node: k='" << k << "'" << std::endl;
        NodeIndex q = rep[k];
        NodeIndex leafToIncrement = 0;
        if(q <= M)
        {
            // a zero weight becomes positive
            interchange_leaves(q, M);

            if(R == 0) {
                R = M / 2;
                if(R > 0) {
                    --E;
                }
            }
            --M;
            --R;
            assert(M == (1ULL << E) + R);

            q = M + 1;
            auto const bq = block[q];
            if constexpr(debug_) std::cout << "\tnew letter" << std::endl;
            if(M > 0) {
                // split the 0-node into an internal node with two children
                // the new 0-node is node M, the old 0-node is node M+1,
                // the parent of nodes M and M+1 is node M+n
                block[M] = bq;
                last[bq] = M;
                auto const oldParent = parent[bq];
                parent[bq] = M + n;
                parity[bq] = 1;
                
                // create a new internal block of zero weight for node M + n
                auto const b = availBlock;
                availBlock = nextBlock[availBlock];
                prevBlock[b] = bq;
                nextBlock[b] = nextBlock[bq];
                prevBlock[nextBlock[bq]] = b;
                nextBlock[bq] = b;
                parent[b] = oldParent;
                parity[b] = 0;
                rtChild[b] = q;
                block[M + n] = b;
                weight[b] = 0;
                first[b] = M + n;
                last[b] = M + n;
                leafToIncrement = q;
                q = M + n;
            }
        } else {
            // interchange q with the first node in its block
            if constexpr(debug_) std::cout << "\tq=" << q << ", block[q]=" << block[q] << ": make leader of block; current leader is " << first[block[q]] << std::endl;

            interchange_leaves(q, first[block[q]]);
            q = first[block[q]];
            if(q == M + 1 && M > 0) {
                leafToIncrement = q;
                q = parent[block[q]];
            }
        }
        return { q, leafToIncrement };
    }

    // subroutine of update
    NodeIndex slide_and_increment(NodeIndex const q) {
        auto const bq = block[q];
        if constexpr(debug_) std::cout << "slide_and_increment: q=" << q << ", block[q]=" << bq << std::endl;

        auto nbq = nextBlock[bq];
        auto const par = parent[bq];
        auto oldParent = par;
        auto oldParity = parity[bq];

        bool slide;
        if((q <= n && first[nbq] > n && weight[nbq] == weight[bq]) || (q > n && first[nbq] <= n && weight[nbq] == weight[bq] + 1)) {
            // slide q over the next block
            if constexpr(debug_) std::cout << "\tslide node over next block" << std::endl;
            slide = true;
            oldParent = parent[nbq];
            oldParity = parity[nbq];

            // adjust child pointers for next higher level in tree
            if(par > 0) {
                auto const bpar = block[par];
                if(rtChild[bpar] == q) {
                    rtChild[bpar] = last[nbq];
                } else if(rtChild[bpar] == first[nbq]) {
                    rtChild[bpar] = q;
                } else {
                    ++rtChild[bpar];
                }

                if(par != Z) {
                    if(block[par+1] != bpar) {
                        if(rtChild[block[par+1]] == first[nbq]) {
                            rtChild[block[par+1]] = q;
                        } else if(block[rtChild[block[par+1]]] == nbq) {
                            ++rtChild[block[par+1]];
                        }
                    }
                }
            }

            // adjust parent pointers for block nbq
            parent[nbq] = parent[nbq] - 1 + parity[nbq];
            parity[nbq] = 1 - parity[nbq];
            nbq = nextBlock[nbq];
        } else {
            slide = false;
        }

        if(((q <= n && first[nbq] <= n) || (q > n && first[nbq] > n)) && weight[nbq] == weight[bq] + 1) {
            // merge q into the block of weight one higher
            if constexpr(debug_) std::cout << "\tmerge into block " << nbq << " of weight " << weight[nbq] << std::endl;
            block[q] = nbq;
            last[nbq] = q;
            if(last[bq] == q) {
                // q's old block disappears
                if constexpr(debug_) std::cout << "\t\tblock " << bq << " (of weight " << weight[bq] << ") disappears" << std::endl;
                nextBlock[prevBlock[bq]] = nextBlock[bq];
                prevBlock[nextBlock[bq]] = prevBlock[bq];
                nextBlock[bq] = availBlock;
                availBlock = bq;
            } else {
                if(q > n) {
                    rtChild[bq] = find_child(q-1, 1);
                }

                if(parity[bq] == 0) {
                    --parent[bq];
                }

                parity[bq] = 1 - parity[bq];
                first[bq] = q - 1;
            }
        } else if(last[bq] == q) {
            // q's block is slid forward in the block list
            if constexpr(debug_) std::cout << "\tslide block " << bq << " forward in the block list (slide=" << slide << ")" << std::endl;
            if(slide) {
                prevBlock[nextBlock[bq]] = prevBlock[bq];
                nextBlock[prevBlock[bq]] = nextBlock[bq];
                prevBlock[bq] = prevBlock[nbq];
                nextBlock[bq] = nbq;
                prevBlock[nbq] = bq;
                nextBlock[prevBlock[bq]] = bq;
                parent[bq] = oldParent;
                parity[bq] = oldParity;
            }
            ++weight[bq];
        } else {
            // a new block is created for q
            if constexpr(debug_) std::cout << "\tcreate new block " << availBlock << " of weight " << (weight[bq] + 1) << std::endl;
            auto const b = availBlock;
            availBlock = nextBlock[availBlock];
            block[q] = b;
            first[b] = q;
            last[b] = q;
            if(q > n) {
                rtChild[b] = rtChild[bq];
                rtChild[bq] = find_child(q-1, 1);
                if(rtChild[b] == q-1) {
                    parent[bq] = q;
                } else if(parity[bq] == 0) {
                    --parent[bq];
                }
            } else if(parity[bq] == 0) {
                --parent[bq];
            }

            first[bq] = q-1;
            parity[bq] = 1 - parity[bq];

            // insert q's block in its proper place in the block list
            prevBlock[b] = prevBlock[nbq];
            nextBlock[b] = nbq;
            prevBlock[nbq] = b;
            nextBlock[prevBlock[b]] = b;
            weight[b] = weight[bq] + 1;
            parent[b] = oldParent;
            parity[b] = oldParity;
        }

        // move q one level higher in the tree
        return (q <= n) ? oldParent : par;
    }

public:
    Vitter87(size_t const alphabet_size) : n(alphabet_size) {
        // allocate
        // nb: this is transferred directly from the 1989 PASCAL implementation, which uses 1-based indexing
        // we allocate one extra item for each of the arrays, wasting an entry each, but only this way we can verify the transferred code for now
        alpha = std::make_unique<Letter[]>(n+1);
        rep = std::make_unique<NodeIndex[]>(n+1);

        block = std::make_unique<BlockIndex[]>(2 * n);

        weight = std::make_unique<size_t[]>(2 * n);
        parent = std::make_unique<NodeIndex[]>(2 * n);
        parity = std::make_unique<bool[]>(2 * n);
        rtChild = std::make_unique<NodeIndex[]>(2 * n);
        first = std::make_unique<NodeIndex[]>(2 * n);
        last = std::make_unique<NodeIndex[]>(2 * n);
        prevBlock = std::make_unique<BlockIndex[]>(2 * n);
        nextBlock = std::make_unique<BlockIndex[]>(2 * n);

        // Initialize
        M = 0;
        E = 0;
        R = -1; // nb: becomes >= 0 if the alphabet has at least one character
        Z = 2 * n - 1;

        for(size_t i = 1; i <= n; i++) {
            ++M;
            ++R;
            if(2 * R == M) {
                ++E;
                R = 0;
            }

            alpha[i] = i;
            rep[i] = i;
        }
        assert(M == (1ULL << E) + R);

        block[n] = 1;

        prevBlock[1] = 1;
        nextBlock[1] = 1;
        weight[1] = 0;
        first[1] = n;
        last[1] = n;
        parity[1] = 0;
        parent[1] = 0;

        availBlock = 2;
        for(auto i = availBlock; i <= Z - 1; i++) {
            nextBlock[i] = i + 1;
        }
        nextBlock[Z] = 0;
    }

    HuffmanCode encode_and_transmit(Letter const j) const {
        // nb: we assume that a Huffman code fits in 64 bits...
        uint64_t stack = 0;

        size_t t;
        NodeIndex q = rep[j]; // nb: j is 1-based!
        size_t i = 0;
        if(q <= M) {
            // encode letter of zero weight
            --q;
            if(q < 2 * R) {
                t = E + 1;
            } else {
                q = q - R;
                t = E;
            }

            for(size_t ii = 1; ii <= t; ii++) {
                ++i;
                stack = (stack << 1) | (q % 2);
                q /= 2;
            }
            q = M;
        }

        NodeIndex const root = (M == n) ? n : Z;
        size_t const w = (q != root) ? weight[block[q]] : 0; // FIXME: for debugging only
        NodeIndex const start = q; // FIXME: for debugging only
        while(q != root) {
            // traverse up the tree
            ++i;
            stack = (stack << 1) | ((first[block[q]] - q + parity[block[q]]) % 2);
            q = parent[block[q]] - (first[block[q]] - q + 1 - parity[block[q]]) / 2;
        }

        // transmit code
        if constexpr(debug_) std::cout << "TRANSMIT '" << j << "' (0x" << std::hex << (size_t)j << std::dec << ") -> " << i << " bits (weight=" << w << ", code=0x" << std::hex << stack << std::dec << ")" << std::endl << std::endl;
        return HuffmanCode{ stack, (uint8_t)i };
    }

    template<typename Receive>
    Letter receive_and_decode(Receive receive) const {
        size_t r = 0; // DEBUG
        size_t q = (M == n) ? n : Z; // set q to the root node
        while(q > n) {
            // traverse down the tree
            q = find_child(q, receive());
            if constexpr(debug_) ++r;
        }

        if(q == M) {
            // decode 0-node
            q = 0;
            for(size_t i = 1; i <= E; i++) {
                q = 2 * q + receive();
                if constexpr(debug_) ++r;
            }

            q = (q < R) ? (2 * q + receive()) : (q + R);
            if constexpr(debug_) ++r;
            ++q;
        }
        if constexpr(debug_) std::cout << "RECEIVE " << r << " bits -> '" << alpha[q] << "' (0x" << std::hex << (size_t)alpha[q] << std::dec << ")" << std::endl << std::endl;
        return alpha[q];
    }

    void update(Letter const k) {
        // Update
        if constexpr(debug_) std::cout << "UPDATE" << std::endl;
        auto [q, leafToIncrement] = find_node(k);
        if constexpr(debug_) std::cout << "q=" << q << ", block[q]=" << block[q] << ", weight[block[q]]=" << weight[block[q]] << std::endl;
        while(q > 0) {
            // at this point, q is the first node in its block
            // increment q's weight by 1 and slide q if necessary over the next block to maintain the invariant
            // then set q to the node one level higher that needs incrementing next
            q = slide_and_increment(q);
        }

        // finish up some special cases involving the 0-node
        if(leafToIncrement != 0) {
            if constexpr(debug_) std::cout << "leafToIncrement=" << leafToIncrement << std::endl;
            slide_and_increment(leafToIncrement);
        }

        if constexpr(debug_) {
            std::cout << "blocks:" << std::endl;
            {
                NodeIndex const root = (M == n) ? n : Z;
                auto b = block[root];
                while(weight[b]) {
                    std::cout << "[" << b << "] weight=" << weight[b] << ", first=" << first[b] << ", last=" << last[b] << std::endl;
                    b = prevBlock[b];
                }
            }
        }

        if constexpr(debug_) std::cout << std::endl;
    }

    void print_debug() const {
        std::cout << std::endl << "TREE:" << std::endl;
        std::cout << "M=" << M << ", E=" << E << ", R=" << R << ", Z=" << Z << std::endl;


        std::cout << std::endl;
    }
};
