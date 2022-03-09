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



def trueMatches(truthtable, IDIndex, IDQuery, NNtable):
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
    if indexpart == 'A':
        for i,j in truthtable:
            if IDIndex.index(i) in NNtable[IDQuery.index(j)]:
                nCandidates +=1
    if indexpart == 'B':
        for j,i in truthtable:
            if IDIndex.index(i) in NNtable[IDQuery.index(j)]:
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
    onceabove = False
    print("start value: {}, start step: {}".format(nn, step))
    index, indextime,indexmemory = createIndex(EmbIndex, indexstring, metric)
    D,I,searchtime,searchmemory = search(EmbQuery, index, nn, probe)
    matches = trueMatches(truthtable, IDIndex, IDQuery, I)
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
        D,I,searchtime,searchmemory = search(EmbQuery, index, nn, probe)
        matches = trueMatches(truthtable, IDIndex, IDQuery, I)
        recall = matches / len(truthtable)

        print(recall, nn)

    return nn, step, recall



if __name__ == '__main__':
    
    #Main infos for starting
    main_dir = '../Embeddings/Processed/' #folder of the datasets and embeddings
    filenumber = 3                      #which dataset should be used

    
    #for filenumber in range(1,10):
    if filenumber in (1,2,5,6):
        columns = ["Embedded Name", "Embedded Clean Name", "Embedded Ag.Value", "Embedded Clean Ag.Value"]
    if filenumber in (3,4,7,8,9,10):
        columns = ["Embedded Title", "Embedded Clean Title", "Embedded Ag.Value", "Embedded Clean Ag.Value"]
    print("\nnow starting with Dataset {}\n".format(filenumber))    
    #loop through all columns "Embedded Title"  "Embedded Clean Title"
    
    #metric: faiss.METRIC_L2 or faiss.METRIC_INNER_PRODUCT
    #normed: True or False
    #nn
    #index- and querypart: 'A' and 'B', reversed if indexpart is 'B'
    for column in columns:
        if column == columns[0]:
            metric = faiss.METRIC_L2
            normed = True
            nn = 39
            indexpart = 'B'
            querypart = 'A'
        if column == columns[1]:
            metric = faiss.METRIC_L2
            normed = True
            nn = 31
            indexpart = 'B'
            querypart = 'A'
        if column == columns[2]:
            metric = faiss.METRIC_L2
            normed = True
            nn = 829
            indexpart = 'A'
            querypart = 'B'
        if column == columns[3]:
            metric = faiss.METRIC_L2
            normed = False
            nn = 574
            indexpart = 'A'
            querypart = 'B'

    
        EmbIndex = np.load(main_dir + 'D{}'.format(filenumber)+indexpart+'-'+column+'.npy')
        EmbQuery = np.load(main_dir + 'D{}'.format(filenumber)+querypart+'-'+column+'.npy')
        IDIndex = list(np.load(main_dir + 'D{}'.format(filenumber)+indexpart+'ID-'+column+'.npy'))
        IDQuery = list(np.load(main_dir + 'D{}'.format(filenumber)+querypart+'ID-'+column+'.npy'))
        truthtable = np.load(main_dir + 'D{}'.format(filenumber)+'GT-'+column+'.npy')
        
        normtime = 0
        
        if normed:
            
            print('Normalizing the dataset')
            temp = time.perf_counter()
            EmbIndex = EmbIndex/np.linalg.norm(EmbIndex, axis=1).reshape(-1, 1)
            EmbQuery = EmbQuery/np.linalg.norm(EmbQuery, axis=1).reshape(-1, 1)
            normtime = time.perf_counter() - temp
            print('Done')
        

        
        #setting up output file:
        outstring = "Output/D{}".format(filenumber)+indexpart+querypart+column+"-NN_"
        if metric ==faiss.METRIC_L2:
            outstring += "L2"
        else: outstring += "IP"
        if normed:
            outstring += "_Normed"
            
        outfile = open(outstring+".txt", 'w')
        outfile.write("NN \t index time \t query time \t normalizing time\
                      \t true matches \t candidates \t all matches \t Index \
                      \t Query \t Searchmemory \t indexmemory\n")
        



        indexstring = 'Flat'
        probe = 1
        
        #if you now what NN is, please comment these three lines and set
        #the range for the loop yourself.

        #maxnn, temp1, temp2 = findNN(EmbIndex, EmbQuery, truthtable, IDIndex, IDQuery, indexstring, 0.9,metric, 100)
        
        index, indextime, indexmemory = createIndex(EmbIndex, indexstring,metric)

        #for i in range(max(maxnn-5,1), maxnn+5,1): 
            #nn = i
           
        print ("Now testing NN: ", nn)
        D,I,searchtime, searchmemory = search(EmbQuery, index, nn)


        matches = trueMatches(truthtable, IDIndex, IDQuery, I)
        outfile.write('{} \t {} \t {} \t {} \
                      \t {} \t {} \t {} \t {} \t {} \t {} \t {} \n'
                      .format(nn, indextime, searchtime, normtime,\
                              matches, len(I)*len(I[0]), len(truthtable),\
                              len(EmbIndex), len(EmbQuery),\
                              searchmemory,indexmemory))
        outfile.close()
