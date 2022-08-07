# Continuous Benchmark of Filtering methods for Entity Resolution

**Entity Resolution** constitutes a core data integration task of *quadratic time complexity*.
As a result, it scales to large datasets through methods that **filtering techniques**, which reduce its computational cost to *candidate pairs*.
These are pairs of similar entity profiles that are highly likely to be matching.
Three types of filtering methods have been proposed in the literature:

 1) Blocking workflows
 2) String similarity joins
 3) Nearest neighbor methods
 
The goal of this repository is to provide code and data for benchmarking the relative performance of the main methods of these three types. 

*We plan to keep our benchmark **up-to-date**, including new filtering methods and datasets shortly after their publication. Please create a new issue in order to request the addition of a new dataset or filtering method.*

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

In our experiments, we thoroughly fine-tune the configuration parameters of these 5 workflows. We also consider 2 baseline workflows:
1) Parameter-free Blocking Workflow, which combines the three parameter-free methods, i.e., Standard Blocking, Block Purging and Comparison Propagation.
2) Default Q-Grams Blocking Workflow, which combines Q-Grams Blocking with Block Filtering and Meta-blocking, using the configuration parameters determined in [a past experimental analysis](http://www.vldb.org/pvldb/vol9/p684-papadakis.pdf).

All code and data are available [here](blockingWorkflows).

## String similarity joins

The following state-of-the-art string similarity join algorithms are considered:
1) ε-Join
2) kNN-Join

In our experiments, we thoroughly fine-tune the configuration parameters of these five workflows. We also consider one baseline method:
1) Default kNN-Join

All code and data are available [here](joins).

## Nearest neighbor methods

The following state-of-the-art NN methods are considered:

1) MinHash LSH
2) Crosspolytope LSH
3) Hyperplane LSH
4) FAISS
5) SCANN
6) DeepBlocker

In our experiments, we thoroughly fine-tune the configuration parameters of these 6 methods. We also consider one baseline method:
1) Default DeepBlocker (DDB)

All code and data are available [here](nnmethods).

### Datasets

For the time being, the following real-world datasets for Clean-Clean ER have been used in our experimental study:

| Dataset Name | D1 Entities | D2 Entities | D1 Name-Value Pairs | D2 Name-Value Pairs | Duplicates | Average NVP per Entity | Brute-force Comparisons |
| --- | --- | --- | --- | --- | --- |--- | --- | 
| D1 (Restaurants) | 339 | 2,256 | 1,130 | 7,519 | 89 | 3.3 | 7.64E+05 |
| D2 (Abt-Buy) | 1,076 | 1,076 | 2,568 | 2,308 | 1,076 | 2.4 | 1.16E+06 |
| D3 (Amazon-Google Products) | 1,354 | 3,039 | 5,302 | 9,110 | 1,104 | 3.9 | 4.11E+06 |
| D4 (DBLP-ACM) | 2,616 | 2,294 | 10,464	| 9,162 | 2,224 | 4.0 | 6.00E+06 | 
| D5 (IMDB-TMDB) | 5,118 | 6,056 | 21,294 | 23,761 | 1,968 | 4.0 | 3.10E+07 | 
| D6 (IMDB-TVDB) | 5,118 | 7,810 | 21,294 | 20,902 | 1,072 | 3.2 | 4.00E+07 |
| D7 (TMDB-TVDB) | 6,056 | 7,810 | 23,761 | 20,902 | 1,095 | 2.2 | 4.73E+07 | 
| D8 (Amazon-Walmart) | 2,554 | 22,074 | 14,143 | 114,315 | 853 | 5.2 | 5.64E+07 | 
| D9 (DBLP-Scholar) | 2,516 | 61,353 | 10,064 | 198,001 | 2,308 | 4.0 | 1.54E+08 | 
| D10 (Movies) | 27,615 | 23,182 | 155,436 | 816,009 | 22,863 | 5.6 | 6.40E+08|

We have also included the following synthetic datasets for Dirty ER in the scalability analysis of our experimental study:

| Dataset Name | Entities | Name-Value Pairs | Duplicates | Average NVP per Entity | Brute-force Comparisons |
| --- | --- | --- | --- | --- | --- |
| D1OK| 10,000 | 106,108 | 8,705 | 10.61 | 5.00E+07 |
| D50K | 50,000 | 530,854 | 43,071 | 10.62 | 1.25E+09 |
| D100K | 100,000 | 1,061,421 | 85,497 | 10.61 | 5.00E+09 |
| D200K | 200,000 | 2,123,728 | 172,403	| 10.62 | 2.00E+10 | 
| D300K | 300,000 | 3,184,885 | 257,034 | 10.62 | 4.50E+10 |
| D1M | 1,000,000 | 10,617,729 | 857,538 | 10.62 | 5.00E+11 |
| D2M | 2,000,000 | 21,238,252 | 1,716,102 | 10.62 | 2.00E+12 |
  
### Technical report

More details are provided in the following technical report:

*George Papadakis, Marco Fisichella, Franziska Schoger, George Mandilaras, Nikolaus Augsten, Wolfgang Nejdl:
How to reduce the search space of Entity Resolution: with Blocking or Nearest Neighbor search?"* ([pdf](https://arxiv.org/abs/2202.12521)).
