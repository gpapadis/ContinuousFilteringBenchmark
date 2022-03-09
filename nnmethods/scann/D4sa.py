# -*- coding: utf-8 -*-
"""
Created on Fri Oct  8 13:34:14 2021

@author: user
"""
import math
import numpy as np
import scann
import pandas as pd
import time
import psutil
from datetime import datetime

def createAHIndex(data, k, similarity):
    starttime = datetime.now()
    memorystart = psutil.Process().memory_info().rss / (1024 * 1024)
    
    normalized_dataset = data / np.linalg.norm(data, axis=1)[:, np.newaxis]

    searcher = scann.scann_ops_pybind.builder(normalized_dataset, k, similarity).tree(
    num_leaves=55, num_leaves_to_search=55, training_sample_size=100).score_ah(dimensions_per_block=2).build()

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
    num_leaves=55, num_leaves_to_search=55, training_sample_size=100).score_brute_force(quantize=False).build()

    memorystop = psutil.Process().memory_info().rss / (1024 * 1024)
    stoptime = datetime.now()
    time_diff = (stoptime - starttime)
    msec = time_diff.total_seconds() * 1000
    return(searcher, msec, memorystop-memorystart)

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

if __name__ == '__main__':
    
    #Main infos for starting
    main_dir = '/home/gap2/Documents/blockingNN/data/csvProfiles/' #folder of the datasets and embeddings
    #column = 'Embedded Ag.Value'            #column with the embeddings to try
    column = 'Embedded Clean Ag.Value'            #column with the embeddings to try
    filenumber = 4                       #which dataset should be used
    indexpart = 'A'                      #which part of the dataset is used
                                         #for index creating
    querypart = 'B'                      #which part of the dataset is used
                                         #for querying
    deli = '%'                           #delimiter in the files
    
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
    
    IDIndex = np.arange(len(IDIndex))
    IDQuery = np.arange(len(IDQuery))
    
    truthIndex, truthQuery = fileread(filetruth, 'EntityFromPart', deli, True, indexpart, querypart)

    truthtable = []
    for i in range(len(truthIndex)):
        truthtable.append((truthIndex[i],truthQuery[i]))
    
    outfile = open(main_dir+"results/Dataset{}".format(filenumber)+column+indexpart+querypart+".txt", 'a')
    outfile.write("NN \t index time \t query time \
                  \t true matches \t candidates \t all matches \t Index \
                  \t Query \t Searchmemory \t indexmemory\n")
    
    k = 1
    print ("Now testing k: ", k)
    for iteration in range(0, 10):
        index, indextime, indexmemory = createBFIndex(EmbIndex, k, "squared_l2")
        neighbors, distances, searchtime, searchmemory = search(EmbQuery, index)
    
        matches = trueMatches(truthtable, IDIndex, IDQuery, neighbors)

        outfile.write('{} \t {:.2f} \t {:.2f} \
                          \t {} \t {} \t {} \t {} \t {} \t {} \t {} \n'
                          .format(k, indextime, searchtime,\
                                  matches, len(neighbors)*len(neighbors[0]), len(truthtable),\
                                  len(EmbIndex), len(EmbQuery),\
                                  searchmemory,indexmemory))
         
    outfile.close()
