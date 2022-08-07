# -*- coding: utf-8 -*-
"""
Created on Fri Oct  8 13:34:14 2021

@author: user
"""

import numpy as np
import faiss
import pandas as pd
import time
import psutil


def createIndex(data, indexstring, metric = faiss.METRIC_L2, efConstruction=-1):
    """
    creates from the data (np.array) the index specified in the indexstring
    (string), if the index is not trained, it will be trained on the data given,
    if an HNSW index is used, you can set the efConstruction value. If the 
    default value (-1) is kept, the efConstruction is not set (good if not using
    HNSW)
    returns the trained index, the time to build the index and the memory consumed

    """
    starttime = time.perf_counter()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    d = len(data[0])                  #dimensionality of the embedding vectors
    index = faiss.index_factory(d, indexstring, metric)         #creation of the index
    if not index.is_trained:
        print("Index is not trained, training now")
        index.train(data)
    print("Index is now trained")
    if efConstruction!=-1:
        index.hnsw.efConstruction = efConstruction
    index.add(data)
    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = time.perf_counter()
    print("Building the index took {:.2f}s".format(stoptime-starttime))
    return(index, stoptime-starttime, memorystop-memorystart)

def search(data, index, nNeighbours, probes = 1):
    """
    given the search query (data, np.array) it searches the index. If the index
    is an IVF index, it visits probes number of cells.
    it returns the distance table D and the ID table I for the number of nearest
    neighbours specified in nNeighbours. The number of a row is the ID of the
    query vector, while the entries are the distances to / IDs of the index
    vectors

    """
    starttime = time.perf_counter()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    index.nprobe = probes
    D,I = index.search(data, nNeighbours)
    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = time.perf_counter()
    print("Searching took {:.2f} s".format(stoptime-starttime))
    return D,I, stoptime-starttime, memorystop-memorystart
    
def searchRunningIndex(data,IDs, nNeighbours, indexstring, probes=1, metric=faiss.METRIC_L2, efConstruction=-1):
    """
    Search with a running index. Take the one entry from data, use that as query and the rest as index
    """
    overall_indextime = 0
    overall_querytime = 0
    allpairs = set([])
    for ID, query in enumerate(data):
        if ID == len(data)-1:
            break
        index_data = data[ID+1:]#np.vstack((data[:ID],data[ID+1:]))
        print(index_data.shape)
        index, indextime, _ = createIndex(index_data, indexstring, metric, efConstruction)
        overall_indextime += indextime
        D, I, querytime, _ = search(np.array([query]), index, nNeighbours, probes)
        overall_querytime += querytime
        query_ID = IDs[ID]
        pair_time = time.perf_counter()
        for index_ID in I[0]:
            #if index_ID < query_ID:
            #    allpairs.add((IDs[index_ID], query_ID))
            #else:
            allpairs.add((query_ID, IDs[index_ID+query_ID]))
        pair_time = time.perf_counter()-pair_time
    return allpairs, overall_indextime, overall_querytime, pair_time 
    
def outputPairs(IndexID, QueryID, NNtable):
    """
    Create the pairs for the output. Given the table of the indices of the 
    Nearest Neighbours and the table of the distances returned by faiss, as well
    as the IDs of the Index and the Query data, it creates a dictionary, where the keys are
    the output pairs (ID1, ID2), where always ID1 < ID2, the values are the distances.
    The dictionary is sorted by the distances.
    """
    pairs = set([])
    for i in range(len(QueryID)):
        for j in NNtable[i]:
            if QueryID[i] < IndexID[j]:
                pairs.add((QueryID[i], IndexID[j]))
            elif IndexID[j] < QueryID[i]: 
                pairs.add((IndexID[j], QueryID[i]))
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
    

def findNN (EmbIndex, EmbQuery, truthtable, IDIndex, IDQuery,indexstring, treshold, metric = faiss.METRIC_L2, startstep=100, probe=1):
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
    once_above = False
    once_below = False
    print("start value: {}, start step: {}".format(nn, step))
    index, indextime,indexmemory = createIndex(EmbIndex, indexstring, metric)
    D,I,searchtime,searchmemory = search(EmbQuery, index, nn, probe)
    pairs = outputPairs(IDIndex, IDQuery, I)
    matches = trueMatchesPairs(truthtable, pairs)
    recall = matches / len(truthtable)
    print(recall, nn)
    while True:
        if (recall>=0.9 and (step==1 or nn==1)):
            break
        if recall > treshold:
            once_above = True
            if once_below:
                step = max(int(step/2),1)
            else:
                step=step*2
            nn = max(nn-step, 1)
        else:
            once_below = True
            if once_above:
                step=max(int(step/2),1)
            else:
                step = step*2
            nn+= step
        D,I,searchtime,searchmemory = search(EmbQuery, index, nn, probe)
        pairs = outputPairs(IDIndex, IDQuery, I)
        matches = trueMatchesPairs(truthtable, pairs)
        
        if recall == matches / len(truthtable):
            break
        recall = matches / len(truthtable)

        print(recall, nn)

    return nn, step, recall



if __name__ == '__main__':
    main_dir = '/home/data/' #folder of the datasets and embeddings
    column = 'Embedded Clean Ag.Value'
    
    for filenumber in ['10K', '50K', '100K', '200K', '300K', '1M', '2M']:  #which dataset should be used
        print("\nnow starting with Dataset \n" + filenumber)
        normed = True
        metric = faiss.METRIC_L2

        EmbIndex = np.load(main_dir + filenumber +'-'+column+'.npy')

        IDIndex = list(np.load(main_dir + filenumber+'ID-'+column+'.npy'))

        truthtable = np.load(main_dir + filenumber+'GT-'+column+'.npy')
        GT = set([])
        for i, entry in enumerate(truthtable):
            GT.add(tuple(truthtable[i]))
        
        normtime = 0
        if normed:
            print('Normalizing the dataset')
            temp = time.perf_counter()
            EmbIndex = EmbIndex/np.linalg.norm(EmbIndex, axis=1).reshape(-1, 1)
            normtime = time.perf_counter() - temp
            print('Done')

        #setting up output file:
        outstring = main_dir + "Output/FAISS"+filenumber+'-Sort-'+column+"-NN_" + "IVF" + "_"
        if metric ==faiss.METRIC_L2:
            outstring += "L2"
        else: outstring += "IP"
        if normed:
            outstring += "_Normed"
            
        outfile = open(outstring+".txt", 'a')
        outfile.write("NN \t probe \t ncells \t index time \t query time \t pair sorting time\t norm time\
                      \t true matches \t candidates \t all matches \t Index \n")
        outfile.close()
        outfile = open(outstring+".txt", 'a')
        
        ncells = 1600
        probe = 80
        for iteartion in range(0,10):
            indexstring = 'IVF{},Flat'.format(ncells)
                
            index, indextime, indexmemory = createIndex(EmbIndex, indexstring,metric)

            nn = 104
           
            print ("Now testing NN: ", nn)
            D,I,searchtime, searchmemory = search(EmbIndex, index, nn,probe)
            
            
            
            pairtime = time.perf_counter()
            pairs = outputPairs(IDIndex, IDIndex, I)
            pairtime = time.perf_counter()-pairtime
            
            matches = trueMatchesPairs(GT, pairs)
            outfile = open(outstring+".txt", 'a')
            outfile.write('{}\t{}\t{} \t {} \t {} \t {} \t {}\
                          \t {} \t {} \t {} \t {} \n'
                          .format(nn,probe, ncells, indextime, searchtime, pairtime, normtime,\
                                  matches, len(list(pairs)), len(truthtable),\
                                      len(EmbIndex)))
            outfile.close()
