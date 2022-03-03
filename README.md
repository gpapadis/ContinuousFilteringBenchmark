# Continuous Benchmark of Filtering methods for Entity Resolution

**Entity Resolution** constitutes a core data integration task of *quadratic time complexity*.
As a result, it scales to large datasets through methods that **filtering techniques**, which reduce its computational cost to *candidate pairs*.
These are pairs of similar entity profiles that are highly likely to be matching.
Three types of filtering methods have been proposed in the literature:

 1) Blocking workflows
 2) String similarity joins
 3) Nearest neighbor methods
 
The goal of this repository is to provide and data for benchmarking the relative performance of the main methods of these three types.

## Blocking Workflows

Each blocking workflow consists of four steps:

1) Block Building, which is a mandatory step that produces a set of blocks. The following methods have been considered so far:
    1)  Standard Blocking
    2)  Q-Grams Blocking
    3)  Extended Q-Grams Blocking
    4)  Suffix Arrays Blocking
    5)  Extended Suffix Arrays Blocking
2) Blocking Purging, which is an *optional, parameter-free* step that discards blocks with more than half the input entities.
3) Block Filtering, which is an *optional* step that retains every entity in r% of its smallest blocks.
4) Comparisonn Cleaning, which is a mandatory step that applies one of the following methods:
    1) Comparison Propagation, which simply cleans a set of overlapping blocks from all repeated candidate pairs.
    2) Meta-blocking, which assigns a score to every candidate pairs that is proportional to its matching likelihoood and thenn discards the lowest weighted pairs. This way, it removes all repeated candidate pairs as well as a large portion of the non-matching ones. Meta-blocking consists of two parts:
        1) A weighting scheme, the scoring function
        2) A pruning algorithm, which cleans the candidate pairs

In our experiments, we thoroughly fine-tune the configuration parameters of these five workflows. We also consider two baseline workflows:
1) Parameter-free Blocking Workflow, which combines the three parameter-free methods, i.e., Standard Blocking, Block Purging and Comparison Propagation.
2) Default Q-Grams Blocking Workflow, which combines Q-Grams Blocking with Block Filtering and Meta-blocking, using the configuration parameters determined in [a past experimental analysis](http://www.vldb.org/pvldb/vol9/p684-papadakis.pdf).

## String similarity joins

## Nearest neighbor methods

### Citation

If you use the code, please cite the following paper:

*George Papadakis, Marco Fisichella, Franziska Schoger, George Mandilaras, Nikolaus Augsten, Wolfgang Nejdl:
How to reduce the search space of Entity Resolution: with Blocking or Nearest Neighbor search?"* ([pdf](https://arxiv.org/abs/2202.12521)).
