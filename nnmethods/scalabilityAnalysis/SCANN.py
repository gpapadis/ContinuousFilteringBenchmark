# -*- coding: utf-8 -*-
"""
Created on Fri Oct  8 13:34:14 2021
@author: user
"""
import numpy as np
import scann

import psutil
from datetime import datetime

def createAHIndex(data, k, similarity):
    starttime = datetime.now()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    
    normalized_dataset = data / np.linalg.norm(data, axis=1)[:, np.newaxis]

    searcher = scann.scann_ops_pybind.builder(normalized_dataset, k, similarity).tree(
    num_leaves=50, num_leaves_to_search=50, training_sample_size=100).score_ah(dimensions_per_block=2).build()

    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = datetime.now()
    time_diff = (stoptime - starttime)
    msec = time_diff.total_seconds() * 1000
    return(searcher, msec, memorystop-memorystart)

def createBFIndex(data, k, similarity):
    starttime = datetime.now()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    
    normalized_dataset = data / np.linalg.norm(data, axis=1)[:, np.newaxis]

    searcher = scann.scann_ops_pybind.builder(normalized_dataset, k, similarity).tree(
    num_leaves=50, num_leaves_to_search=50, training_sample_size=100).score_brute_force(quantize=False).build()

    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = datetime.now()
    time_diff = (stoptime - starttime)
    msec = time_diff.total_seconds() * 1000
    return(searcher, msec, memorystop-memorystart)

def search(data, searcher):
    """
    given the search query (data, np.array) it searches the index. If the index
    is an IVF index, it visits probes number of cells.
    it returns the distance table D and the ID table I for the number of nearest
    neighbours specified in nNeighbours. The number of a row is the ID of the
    query vector, while the entries are the distances to / IDs of the index
    vectors
    """
    starttime = datetime.now()
    
    normalized_dataset = data / np.linalg.norm(data, axis=1)[:, np.newaxis]
    
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
   
    neighbors, distances = searcher.search_batched(normalized_dataset)

    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = datetime.now()
    time_diff = (stoptime - starttime)
    msec = time_diff.total_seconds() * 1000
    return neighbors, distances, msec, memorystop-memorystart


def trueMatches(truthtable, IDIndex, IDQuery, NNtable, Querypart):
    """
    given a set of nearest neighbours in the NNtable, it calculates how many
    can also be found in the truthtable (list of tuples) given. IDIndex and Query
    contain the IDs of the index and query vectors. Those IDs are not included
    when creating the index or searching, so the results have to be mapped to
    the original index to use the truthtable.
    """
    nCandidates = 0
    IDIndex = list(IDIndex)
    IDQuery = list(IDQuery)
    if Querypart == 'B':
        for i,j in truthtable:
            if IDIndex.index(i) in NNtable[IDQuery.index(j)]:
                nCandidates +=1
    else:
        for j,i in truthtable:
            if IDIndex.index(i) in NNtable[IDQuery.index(j)]:
                nCandidates +=1
    return nCandidates

def outputPairs(DataID, NNtable):
    """
    Create the pairs for the output. Given the table of the indices of the 
    Nearest Neighbours and the table of the distances returned by faiss, as well
    as the IDs of the Index and the Query data, it creates a dictionary, where the keys are
    the output pairs (ID1, ID2), where always ID1 < ID2, the values are the distances.
    The dictionary is sorted by the distances.
    """
    pairs = set([])
    for i in range(len(DataID)):
        for j in NNtable[i]:
            if DataID[i] < DataID[j]:
                pairs.add((DataID[i], DataID[j]))
            elif DataID[j] < DataID[i]: 
                pairs.add((DataID[j], DataID[i]))
    return pairs
    
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

def findNN (EmbData, truthtable, IDData, indexstring, treshold, similarity, startstep=100):
    """
    finds (approximately) the number of nearest neigbours, for which the recall is
    above the treshold. The index is build from EmbIndex like specified in
    indexstring, the query is EmbQuery, from the truthtable and IDIndex and
    IDQuery the number of true matches is calculated. In the beginning the 
    algorithm increases the number of nearest neighbours by startstep.
    Returns the number of nearest neighbors, the stepsize at the end and the
    recall


    """
    print("starting the search")
    nn = 1
    step =startstep
    recall = 0
    onceabove = False
    print("start value: {}, start step: {}".format(nn, step))
    if indexstring == 'AH':
        index, indextime,indexmemory = createAHIndex(EmbData, nn, similarity)
    if indexstring == 'BF':
        index, indextime,indexmemory = createBFIndex(EmbData, nn, similarity)
    neighbors,distances,searchtime,searchmemory = search(EmbData, index)
    
    pairs = outputPairs(IDData, neighbors)
    matches = trueMatchesPairs(truthtable, pairs)
    recall = matches / len(truthtable)
    print(recall, nn)
    while ((step > 2 or recall < treshold) and nn>0):
        if recall > treshold:
            onceabove = True
            if nn == 1: break
            step = max(int(step/2),1)
            nn = nn - step
        else:
            if onceabove: step=max(int(step/2),1)
            nn = nn + step
            
        if indexstring == 'AH':
            index, indextime,indexmemory = createAHIndex(EmbData, nn, similarity)
        if indexstring == 'BF':
            index, indextime,indexmemory = createBFIndex(EmbData, nn, similarity)
        neighbors,distances,searchtime,searchmemory = search(EmbData, index)
        pairs = outputPairs(IDData, neighbors)
        matches = trueMatchesPairs(truthtable, pairs)
        recall = matches / len(truthtable)

        print(recall, nn)

    return nn, step, recall


if __name__ == '__main__':
    main_dir = '/home/data/' #folder of the datasets and embeddings
    column = 'Embedded Clean Ag.Value'            #column with the embeddings to try
    indexpart = 'B'                      #which part of the dataset is used
                                         #for index creating
    querypart = 'A'                      #which part of the dataset is used
                                         #for querying
    deli = '|'                           #delimiter in the files
    k = 37
    
    for filenumber in ['10K', '50K', '100K', '200K', '300K', '1M', '2M']:  #which dataset should be used
        #creating the filenames from the infos given above
        EmbIndex = np.load(main_dir + filenumber +'-'+column+'.npy')

        IDIndex = list(np.load(main_dir + filenumber+'ID-'+column+'.npy'))
        
        truthtable = np.load(main_dir + filenumber+'GT-'+column+'.npy')
        GT = set([])
        for i, entry in enumerate(truthtable):
            GT.add(tuple(truthtable[i]))

        outfile = open(main_dir + "Output/SCANN"+filenumber+column+".txt", 'a')
        outfile.write("NN \t index time \t query time \t pairing time \
                      \t true matches \t candidates \t all matches \t Index \
                      \t Searchmemory \t indexmemory \t indexstring \t similarity\n")
        outfile.close()
        
        similarity = "squared_l2"
        indexstring = "AH"
        for iteartion in range(0,10):
            outfile = open(main_dir + "Output/SCANN"+filenumber+column+".txt", 'a')
            print ("Now testing k: ", k)   
            
            if indexstring == 'AH':
                index, indextime,indexmemory = createAHIndex(EmbIndex, k, similarity)
            if indexstring == 'BF':
                index, indextime,indexmemory = createBFIndex(EmbIndex, k, similarity)
                
            neighbors, distances, searchtime, searchmemory = search(EmbIndex, index)
            starttime = datetime.now()
            pairs = outputPairs(IDIndex, neighbors)
            stop_time = datetime.now()
            delta = (stop_time-starttime)
            pairing_time = delta.total_seconds() * 1000
            matches = trueMatchesPairs(GT, pairs)
    
            outfile.write('{} \t {:.2f} \t {:.2f} \t {:.2f} \
                              \t {} \t {} \t {} \t {} \t {} \t {}  \t {} \t {}\n'
                              .format(k, indextime, searchtime, pairing_time,\
                                      matches, len(pairs), len(truthtable),\
                                      len(EmbIndex),\
                                      searchmemory,indexmemory, indexstring, similarity))
                
            outfile.close()
