#from scipy.spatial.distance import cosine
import numpy as np
import pandas as pd
import math
import time

MAX_VAL = (2**31)-1
LARGE_PRIME = 433494437
ngrams_index = []
THRESHOLD = 0.5


def compute_sig_size(bands):
    r = math.ceil(math.log(1./bands)/math.log(THRESHOLD))+1
    return r*bands

def create_vocab(data, column, n):
    global ngrams_index
    ngrams_index = []
    preprocess(data, column, n, fixed_vocab = False)

def preprocess(data, column, n, fixed_vocab = True):
    splitted_arr = data[column].astype(str).apply(lambda x: ngrams(x,n, fixed_vocab)).to_numpy()
    
    if fixed_vocab:
        maxlength =len(splitted_arr[-1])
        
        dataset = np.empty((len(splitted_arr), maxlength), dtype=bool)
        dataset.fill(0)
        for i, element in enumerate(splitted_arr):
            dataset[i,:len(splitted_arr[i])]= element.copy()
        del splitted_arr
        return dataset


def ngrams(string, n, fixed_vocab = True):
    global ngrams_index
    num_ngrams = len(string)-n+1
    bool_vec = np.zeros(len(ngrams_index), dtype = bool)
    if len(string) == 0:
        return bool_vec
    
    while num_ngrams<1:
        string+=' '
        num_ngrams = len(string)-n+1
    for i in range(num_ngrams):
        ngram = string[i : i+n]
        if ngram not in ngrams_index:
            if not fixed_vocab:
                ngrams_index.append(ngram)
                bool_vec = np.hstack((bool_vec, 0))
        else:
            bool_vec[ngrams_index.index(ngram)]=1

    return bool_vec

def hash_row(a, b, r):
    return (a*r + b)%MAX_VAL

def signatures(bool_dataset, row_hashes, num_funct):
    
    entries, ngrams = bool_dataset.shape
    output = np.empty((entries,num_funct))
    output.fill(np.inf)
        
    for i in range(entries):
        for j in range(ngrams):
            if bool_dataset[i][j]:
                output[i]=np.min(np.vstack((row_hashes[j], output[i])),axis=0)

    return output

def hashing(signatures, bands, buckets):
    entries, hash_functions = signatures.shape
    assert hash_functions%bands == 0
    rows = int(hash_functions/bands)
    signatures = signatures.reshape(entries, bands, rows)
    hashes = np.sum(signatures, axis=2,dtype='int64')*LARGE_PRIME%buckets
    hash_table = np.zeros((bands, buckets), dtype=object)
    for i in range(entries):
        for j in range(bands):
            if hash_table[j, hashes[i, j]] == 0:
                hash_table[j, hashes[i, j]]=set([i])
            else:
                hash_table[j, hashes[i, j]].add(i)
    return hashes, hash_table

def sorted_pairs(IndexHash_table, QueryHash_vectors, IndexID, QueryID):
    candidates = set([])
    for ID_q, query in enumerate(QueryHash_vectors):
        for band, hash_value in enumerate(query):
            if IndexHash_table[band][hash_value] != 0:
                for member in IndexHash_table[band][hash_value]:
                    if IndexID[member]<QueryID[ID_q]:
                        candidates.add((IndexID[member].copy(),QueryID[ID_q].copy()))
                    elif IndexID[member]>QueryID[ID_q]:
                        candidates.add((QueryID[ID_q].copy(),IndexID[member].copy()))
    return candidates

def get_candidates(IndexHash_table, QueryHash_vectors):
    candidates = np.zeros((2,len(QueryHash_vectors)), dtype=object)
    for ID_q, query in enumerate(QueryHash_vectors):
        for band, hash_value in enumerate(query):
            if IndexHash_table[band][hash_value] != 0:
                if candidates[0][ID_q] == 0:
                    candidates[0][ID_q] = IndexHash_table[band][hash_value].copy()
                    candidates[1][ID_q] = len(candidates[0][ID_q])
                else:
                    candidates[0][ID_q] = candidates[0][ID_q].union(IndexHash_table[band][hash_value])
                    candidates[1][ID_q]= len(candidates[0][ID_q])
    return candidates

def get_unique_pairs(candidates, IndexID, QueryID):
    u_candidates = []
    for id_q, cand in enumerate(candidates[0]):
        for id_i in cand:
            if IndexID[id_i]<QueryID[id_q]:
                u_candidates.append((IndexID[id_i].copy(),QueryID[id_q].copy()))
            elif IndexID[id_i]>QueryID[id_q]:
                u_candidates.append((QueryID[id_q].copy(), IndexID[id_i].copy()))
    return set(u_candidates)
        

def trueMatchesPairs(GT, pairs):
    """
    given a set of nearest neighbours in the NNtable, it calculates how many
    can also be found in the truthtable (list of tuples) given. IDIndex and Query
    contain the IDs of the index and query vectors. Those IDs are not included
    when creating the index or searching, so the results have to be mapped to
    the original index to use the truthtable.
    """
    nCandidates = 0
    GT = set(GT)
    for pair in pairs:
        if pair in GT:
            nCandidates +=1
    return nCandidates

if __name__ == '__main__':
    main_dir = '/home/data/' #folder of the datasets and embeddings
    indexpart = '1'                      #which part of the dataset is used
                                         #for index creating
    querypart = '2'                      #which part of the dataset is used
                                         #for querying
    deli = '|'                           #delimiter in the files
    
    rng = np.random.default_rng(time.perf_counter_ns())
    
    ngrams_size = 2
    buckets = 16
    bands = 32
    column = 'Clean Ag.Value'
    
    for filenumber in ['10K', '50K', '100K', '200K', '300K', '1M', '2M']:  #which dataset should be used
    
        data = pd.read_csv(main_dir + filenumber +'full.csv', sep = deli, engine = 'python', encoding_errors='replace')
        
        dataIDs = data['Id'].to_numpy()
        
        GT = pd.read_csv(main_dir + filenumber +'duplicates.csv', sep = '|', engine = 'python', encoding_errors='replace')
        
        truthindex = GT['Entity'+indexpart].to_numpy()
        truthquery = GT['Entity'+querypart].to_numpy()
     
        truthtable = set([])
        for ID_1, ID_2 in zip(truthindex, truthquery):
            if ID_1 < ID_2:
                truthtable.add((ID_1, ID_2))
            else:
                truthtable.add((ID_2, ID_1))
        
        for iterations in (0, 10):
            t_preprocess = time.perf_counter()
            create_vocab(data, column, ngrams_size)
            dataset= preprocess(data, column, ngrams_size, fixed_vocab=True)
            t_preprocess = time.perf_counter()-t_preprocess
            
            DataIDs = data['Id'].to_numpy()

            outstring = main_dir + "Processed/Output/MHLSH"+filenumber+'-'+column
            outfile = open(outstring+".txt", 'a')
            outfile.write("Bands \t Buckets \t ngram size \t prep \t sig \t hash  \
                          \t build candidate set time \t true matches \t candidates \t all matches \t dataset  \n")

            outfile = open(outstring+".txt", 'a')
            sig_size = compute_sig_size(bands)
            
            a = rng.integers(0, MAX_VAL, (sig_size),endpoint=True)
            a = np.reshape(a, (-1,len(a)))                    
            b = rng.integers(0, MAX_VAL, (sig_size),endpoint=True)

            
            t_sig = time.perf_counter()
            rows = np.arange(1, dataset.shape[1]+1)
            hashed_rows = hash_row(a,b,np.reshape(rows, (len(rows), -1)))
            signature = signatures(dataset, hashed_rows, sig_size)
            t_sig = time.perf_counter()-t_sig
            del rows
            del hashed_rows
            
            t_hash = time.perf_counter()
            hash_vectors, hash_table = hashing(signature, bands, buckets)
            t_hash = time.perf_counter()-t_hash
            del signature
            
            
            t_pairtime = time.perf_counter()        
            #candidates = get_candidates(hash_table, hash_vectors)
            #unique_candidates = get_unique_pairs(candidates, dataIDs, dataIDs)
            unique_candidates = sorted_pairs(hash_table, hash_vectors, dataIDs, dataIDs)
            t_pairtime = time.perf_counter() - t_pairtime
            del hash_table
            del hash_vectors
            
            true_matches = trueMatchesPairs(truthtable, unique_candidates)
            outfile.write('{} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {}\n'
                          .format(bands, buckets, ngrams_size, t_preprocess, t_sig, t_hash,t_pairtime,\
                                  true_matches, len(unique_candidates), len(truthtable),len(dataIDs)))
            outfile.close()