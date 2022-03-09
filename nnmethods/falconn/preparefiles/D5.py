
#!/usr/bin/python

import sys
import struct
import numpy as np
import pandas as pd


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
    print('reading now in file '+filename+ ', column ' + column)
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
        
        
#Main infos for starting
main_dir = '/home/gap2/Documents/blockingNN/data/csvProfiles/' #folder of the datasets and embeddings
filenumber = 5                       #which dataset should be used
indexpart = 'A'                      #which part of the dataset is used
                                     #for index creating
querypart = 'B'                      #which part of the dataset is used
                                     #for querying
deli = '|'                           #delimiter in the files

#creating the filenames from the infos given above
fileIndex = main_dir + 'fastText/D{}'.format(filenumber)+indexpart+'emb.csv'
fileQuery = main_dir + 'fastText/D{}'.format(filenumber)+querypart+'emb.csv'
filetruth = main_dir + 'D{}groundtruth.csv'.format(filenumber)



#loop through all columns "Embedded Title"  "Embedded Clean Title"
for column in ["Embedded Name", "Embedded Clean Name", "Embedded Ag.Value", "Embedded Clean Ag.Value"]:
    
    #reading in the files
    EmbIndex, IDIndex, dubIDIndex = fileread(fileIndex, column, deliminator=deli)
    EmbIndex = np.array(EmbIndex,dtype='float32')
    IDIndex = np.array(IDIndex)
    
    EmbQuery, IDQuery, dubIDQuery = fileread(fileQuery, column, deliminator=deli)
    EmbQuery = np.array(EmbQuery, dtype='float32')
    IDQuery = np.array(IDQuery)
    
    truthIndex, truthQuery = fileread(filetruth, 'EntityFromPart', deli, True, indexpart, querypart)
                                      
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
    

    np.save(main_dir + "falconn/D{}".format(filenumber)+indexpart+"-"+column, np.array(EmbIndex))
    np.save(main_dir + "falconn/D{}".format(filenumber)+indexpart+"ID-"+column, np.array(IDIndex))
    np.save(main_dir + "falconn/D{}".format(filenumber)+querypart+"-"+column, np.array(EmbQuery))
    np.save(main_dir + "falconn/D{}".format(filenumber)+querypart+"ID-"+column, np.array(IDQuery))
    np.save(main_dir + "falconn/D{}".format(filenumber)+"GT-"+column, np.array(truthtable))
