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
from datetime import datetime

main_dir = 'Embeddings/newDatasets'

def fileread(filename, column, deliminator = '|',
             Truthtable =False, indexpart='A', querypart='B'):
    """
    reads in a file and returns data from it as a list.
    
    MANDATORY INPUT:
        filename (string)
        column (string) -> which column do you want the data from
        
    OPTIONAL INPUT:
        deliminator (string, default '|') -> what character separates the
            columns in the file
        Truthtable (boolean, default False) -> instead of a data file, read in
            the ground truth table given as input, modifies the output
        indexpart (string, default 'A') -> only relevant if Truthtable is True
            which part of the dataset was used for index creation
        querypart (string, default 'B') -> only relevant if Truthtable is True
            which part of the dataset was used for query

    OUTPUT:
        if Truthtable is False (default): returns the data from the column as
            list and a second list containing the IDs of the data. The third 
            list contains the IDs of the removed entries
        if Truthtable is True: returns two lists, first the column of the
            indexpart, second the column of the querypart from the groundtruth 
            file

    """
    print('reading now in file '+filename)
    file = pd.read_csv(filename,sep = deliminator, engine = 'python')
    if not Truthtable:
        rawdata = file[column]
        emptyIDs = []
        data = []
        for i in range(len(rawdata)):              #strip the string of a list
                                                 #of any unnecessary characters
                                                 #and divide it into a list of 
                                                 #single strings
            vector = rawdata[i]
            if not vector == '' and not pd.isnull(vector):
                vector = vector.strip('][ ').replace('\n', '').replace('   ', ' ').replace('  ', ' ').split(' ')
                for string in vector:
                    if string == '':                 #remove empty strings
                        vector.remove(string)
                    else:                            #and convert the strings into
                        string = float(string)       #floats
                data.append(vector)
            else:
                emptyIDs.append(i)
        return data, list(file['Id']), emptyIDs  #return the newly created list
                                                 #and the IDs of the items
                                                 #which were originally stored
                                                 #as int
    else:                                        #if Truthtable is True
                                                 #return the entries from the
                                                 #groundtruth table
        return list(file[column+indexpart]), list(file[column+querypart])


def createIndex(data, indexstring, efConstruction=-1):
    """
    creates from the data (np.array) the index specified in the indexstring
    (string), if the index is not trained, it will be trained on the data given,
    if an HNSW index is used, you can set the efConstruction value. If the 
    default value (-1) is kept, the efConstruction is not set (good if not using
    HNSW)
    returns the trained index, the time to build the index and the memory consumed

    """
    starttime = datetime.now()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    d = len(data[0])                  #dimensionality of the embedding vectors
    index = faiss.index_factory(d, indexstring)         #creation of the index
    if not index.is_trained:
        print("Index is not trained, training now")
        index.train(data)
    print("Index is now trained")
    if efConstruction!=-1:
        index.hnsw.efConstruction = efConstruction
    index.add(data)
    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = datetime.now()
    time_diff = (stoptime - starttime)
    msec = time_diff.total_seconds() * 1000
    return(index, msec, memorystop-memorystart)

def search(data, index, nNeighbours, probes = 1):
    """
    given the search query (data, np.array) it searches the index. If the index
    is an IVF index, it visits probes number of cells.
    it returns the distance table D and the ID table I for the number of nearest
    neighbours specified in nNeighbours. The number of a row is the ID of the
    query vector, while the entries are the distances to / IDs of the index
    vectors

    """
    starttime = datetime.now()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    index.nprobe = probes
    D,I = index.search(data, nNeighbours)
    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = datetime.now()
    time_diff = (stoptime - starttime)
    msec = time_diff.total_seconds() * 1000
    return D,I, msec, memorystop-memorystart

def rangesearch(data, index, threshold, probes):
    """
    given the search query (data, np.array) it searches the index. If the index
    is an IVF index, it visits probes number of cells. Not available for HNSW
    index.
    it returns the distance table D and the ID table I for all vectors which
    have a smaller distance than the threshold to the query vectors.
    for i the ID of the queryvector, the distances and IDs of the corresponding
    indices are stored in X[lim[i]:lim[i+1]]

    """
    starttime = time.time()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    index.nprobe = probes
    lim,D,I = index.range_search(data, threshold)
    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = time.time()
    print("Searching took {:.2f} s".format(stoptime-starttime))
    return lim,D,I, stoptime-starttime, memorystop-memorystart


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
    for i,j in truthtable:
        if IDIndex.index(i) in NNtable[IDQuery.index(j)]:
            nCandidates +=1
    return nCandidates

def trueMatchesRange(truthtable, IDIndex, IDQuery, NNtable, lim):
    """
    given the table of range search results (NNtable, lim), it calculates how many
    can also be found in the truthtable (list of tuples) given. IDIndex and Query
    contain the IDs of the index and query vectors. Those IDs are not included
    when creating the index or searching, so the results have to be mapped to
    the original index to use the truthtable.
    """
    nCandidates = 0
    for i,j in truthtable:
        if IDIndex.index(i) in NNtable[lim[IDQuery.index(j)]:lim[IDQuery.index(j)+1]]:
            nCandidates +=1
    return nCandidates

def findNN (EmbIndex, EmbQuery, truthtable, IDIndex, IDQuery,indexstring, treshold, startstep=100):
    """
    finds (approximately) the number of nearest neigbours, for which the recall is
    above the treshold. The index is build from EmbIndex like specified in
    indexstring, the query is EmbQuery, from the truthtable and IDIndex and
    IDQuery the number of true matches is calculated. In the beginning the 
    algorithm increases the number of nearest neighbours by startstep.
    Returns the number of nearest neighbors, the stepsize at the end and the
    recall


    """
    nn = 1
    step =startstep
    recall = 0
    onceabove = False
    index, indextime,indexmemory = createIndex(EmbIndex, indexstring)
    D,I,searchtime,searchmemory = search(EmbQuery, index, nn)
    matches = trueMatches(truthtable, IDIndex, IDQuery, I)
    recall = matches / len(truthtable)
    while (step > 5 or recall < treshold):
        if recall > treshold:
            onceabove = True
            if nn == 1: break
            step = max(int(step/2),1)
            nn = nn - step
        else:
            if onceabove: step=max(int(step/2),1)
            nn = nn + step
        D,I,searchtime,searchmemory = search(EmbQuery, index, nn)
        matches = trueMatches(truthtable, IDIndex, IDQuery, I)
        recall = matches / len(truthtable)

        print(recall, nn)

    return nn, step, recall




if __name__ == '__main__':
    
    #Main infos for starting
    main_dir = '/home/gap2/Documents/blockingNN/data/csvProfiles/' #folder of the datasets and embeddings
    column = 'Embedded Clean Ag.Value'            #column with the embeddings to try
    filenumber = 9                       #which dataset should be used
    indexpart = 'B'                      #which part of the dataset is used
                                         #for index creating
    querypart = 'A'                      #which part of the dataset is used
                                         #for querying
    deli = '>'                           #delimiter in the files
    
    #creating the filenames from the infos given above
    fileIndex = main_dir + 'fastText/D{}'.format(filenumber)+indexpart+'emb.csv'
    fileQuery = main_dir + 'fastText/D{}'.format(filenumber)+querypart+'emb.csv'
    filetruth = main_dir + 'D{}groundtruth.csv'.format(filenumber)
     
    #reading in the files
    EmbIndex, IDIndex, dubIDIndex = fileread(fileIndex, column, deliminator=deli)
    EmbIndex = np.array(EmbIndex,dtype='float32')
    IDIndex = np.array(IDIndex)
    
    EmbQuery, IDQuery, dubIDQuery = fileread(fileQuery, column, deliminator=deli)
    EmbQuery = np.array(EmbQuery, dtype='float32')
    IDQuery = np.array(IDQuery)
    
    truthIndex, truthQuery = fileread(filetruth, 'EntityFromPart', deli,
                                      True, indexpart, querypart)
    #if the ID in the truthtable starts at zero and not at the higher number
    #as in the dataset, please uncomment these lines, otherwise leave commented
    IDIndex = np.arange(len(IDIndex))
    IDQuery = np.arange(len(IDQuery))
    
    truthtable = []
    for i in range(len(truthIndex)):
        if not (list(IDIndex).index(truthIndex[i]) in dubIDIndex or
                list(IDQuery).index(truthQuery[i]) in dubIDQuery):
            truthtable.append((truthIndex[i],truthQuery[i]))
    IDIndex = np.delete(IDIndex, dubIDIndex,0)
    IDQuery = np.delete(IDQuery, dubIDQuery,0)
    
    #setting up output file:
    outfile = open("/home/gap2/Documents/blockingNN/faiss/results/Dataset{}".format(filenumber)+column+indexpart+querypart+"Flat"+".txt", 'a')
    outfile.write("NN \t index time \t query time \
                  \t true matches \t candidates \t all matches \t Index \
                  \t Query \t Searchmemory \t indexmemory\n")
    
    #setting up the loops and starting the search
    
    efConstruction = -1
    indexstring = 'Flat'
    probe = 1  
    nn = 260
    
    print ("Now testing NN: ", nn)
    for iteration in range(0, 10):
        index, indextime, indexmemory = createIndex(EmbIndex, indexstring, efConstruction)
    
        D,I,searchtime, searchmemory = search(EmbQuery, index, nn, probe)
        #lim, D, I,searchtime, searchmemory = rangesearch(EmbQuery, index, nn, probe)
    
        matches = trueMatches(truthtable, IDIndex, IDQuery, I)
        
        #matches = trueMatchesRange(truthtable, list(IDIndex), list(IDQuery), I, lim)
        outfile.write('{} \t {:.2f} \t {:.2f} \
                      \t {} \t {} \t {} \t {} \t {} \t {} \t {} \n'
                      .format(nn, indextime, searchtime,\
                              matches, len(I)*len(I[0]), len(truthtable),\
                              len(EmbIndex), len(EmbQuery),\
                              searchmemory,indexmemory))
    outfile.close()
