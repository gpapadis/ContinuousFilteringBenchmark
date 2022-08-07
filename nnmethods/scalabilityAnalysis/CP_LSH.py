from __future__ import print_function
import numpy as np
import falconn
import timeit
import math
import psutil
import time

def outputPairs(IndexID, QueryID, Queries, query_object):
    """
    Create the pairs for the output. Given the table of the indices of the 
    Nearest Neighbours and the table of the distances returned by faiss, as well
    as the IDs of the Index and the Query data, it creates a dictionary, where the keys are
    the output pairs (ID1, ID2), where always ID1 < ID2, the values are the distances.
    The dictionary is sorted by the distances.
    """
    pairs = set([])
    for i in range(len(QueryID)):
        for j in query_object.get_unique_candidates(Queries[i]):
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

if __name__ == '__main__':

    main_dir = '/home/data/'
    column = 'Embedded Clean Ag.Value'
 
    L = 1
    k = 2
    cp = 1
               
    number_of_tables = 128
    number_of_functions = 2
    cpd = 256
    number_of_probes = 2497
    storage = falconn.StorageHashTable.LinearProbingHashTable    
    
    for datasetfile in ['10K', '50K', '100K', '200K', '300K', '1M', '2M']:  #which dataset should be used
        print('Reading in the dataset', datasetfile)
        
        normed = True
    
        dataset = np.load(main_dir + datasetfile +'-'+column+'.npy')

        ID = list(np.load(main_dir + datasetfile +'ID-'+column+'.npy'))

        truthtable = np.load(main_dir + datasetfile +'GT-'+column+'.npy')
        gt = set([])
        for i, entry in enumerate(truthtable):
            gt.add(tuple(truthtable[i]))
        
        # It's important not to use doubles, unless they are strictly necessary.
        # If your dataset consists of doubles, convert it to floats using `astype`.
        assert dataset.dtype == np.float32

        # Normalize all the lenghts, since we care about the cosine similarity.
        print('Normalizing the dataset')
        dataset /= np.linalg.norm(dataset, axis=1).reshape(-1, 1)
        print('Done')

        # Center the dataset and the queries: this improves the performance of LSH quite a bit.
        print('Centering the dataset and queries')
        center = np.mean(dataset, axis=0)
        dataset -= center
        print('Done')

        # we build only 50 tables, increasing this quantity will improve the query time
        # at a cost of slower preprocessing and larger memory footprint, feel free to
        # play with this number
        outstring = main_dir + "Output/" + datasetfile+ "_CP_"+column+".txt"
        out = open(outstring, 'a')
        out.write("LSH \t dist Funct \t num tables \t num hash funct \t last cp dim \t num probes \t index time \t query time \t true matches \t all candidates \t all matches \t Index \t mem index \t mem query\n")
        out.close()
                
        for iterations in (0, 10):
            #params_cp = falconn.get_default_parameters(len(dataset), len(dataset[0]), falconn.DistanceFunction.EuclideanSquared,False)
            params_cp = falconn.LSHConstructionParameters()
            params_cp.dimension = len(dataset[0])
            params_cp.lsh_family = falconn.LSHFamily.CrossPolytope
            params_cp.distance_function = falconn.DistanceFunction.NegativeInnerProduct 
            params_cp.l = number_of_tables
            # we set one rotation, since the data is dense enough,
            # for sparse data set it to 2
            params_cp.num_rotations = 1
            print(time.perf_counter_ns())
            params_cp.seed = time.perf_counter_ns()      
            # we want to use all the available threads to set up
            params_cp.num_setup_threads = 0
            params_cp.storage_hash_table = storage
            # we build 18-bit hashes so that each table has
            # 2^18 bins; this is a good choise since 2^18 is of the same
            # order of magnitude as the number of data points
            #falconn.compute_number_of_hash_functions(num_hash_bits, params_cp)
            params_cp.k = number_of_functions
            params_cp.last_cp_dimension = cpd
            #print(params_cp.feature_hashing_dimension) # = 2**k
            
            
            print('Number of hash functions: ',params_cp.k)
            #print('Last cp dimension: ', params_cp.last_cp_dimension)


            print('Constructing the LSH table')
            t1 = timeit.default_timer()
            m1= psutil.Process().memory_info().rss / (1024 * 1024)
            table = falconn.LSHIndex(params_cp)
            table.setup(dataset)
            m2 = psutil.Process().memory_info().rss / (1024 * 1024)
            t2 = timeit.default_timer()
            print('Done')
            constructtime = t2-t1
            indexm = m2-m1
            print('Construction time: {}'.format(constructtime))
            

            query_object = table.construct_query_object()
            

            # find the smallest number of probes to achieve accuracy 0.9
            # using the binary search
            print('Choosing number of probes')
            #number_of_probes = params_cp.l

            def evaluate_number_of_probes(number_of_probes):
                query_object.set_num_probes(number_of_probes)
                score = 0
                
                pairs = outputPairs(ID, ID, dataset, query_object)
                score = trueMatchesPairs(gt, pairs)
                return float(score) / len(gt), score

            #while True:
            #    accuracy, score = evaluate_number_of_probes(number_of_probes)
            #    print('{} -> {}'.format(number_of_probes, accuracy))
            #    if accuracy >= 0.9:
            #        break
            #    number_of_probes = number_of_probes * 2
            #if number_of_probes > params_cp.l:
            #    left = number_of_probes // 2
            #    right = number_of_probes
            #    while right - left > 1:
            #        number_of_probes = (left + right) // 2
            #        accuracy, score = evaluate_number_of_probes(number_of_probes)
            #        print('{} -> {}'.format(number_of_probes, accuracy))
            #        if accuracy >= 0.9:
            #            right = number_of_probes
            #        else:
            #            left = number_of_probes
            #    number_of_probes = right
            #print('Done')
            #print('{} probes'.format(number_of_probes))

            # final evaluation
            query_object.set_num_probes(number_of_probes)
            t1 = timeit.default_timer()
            m1 = psutil.Process().memory_info().rss / (1024 * 1024)
            finscore = 0
            pairs = outputPairs(ID, ID, dataset, query_object)
            finscore = trueMatchesPairs(gt, pairs)
            
            m2= psutil.Process().memory_info().rss / (1024 * 1024)
            t2 = timeit.default_timer()
            querym = m2-m1
            querytime = t2-t1
            print('Query time: {}'.format((querytime) / len(dataset)))
            print('Precision: {}'.format(float(finscore) / len(gt)))
            
            #full number of candidates:
            fullCandidates=0
            for query in dataset:
                fullCandidates += len(query_object.get_unique_candidates(query))
            #out.write("{} \t {}".format(num_hash_bits, number_of_tables))
            out = open(outstring, 'a')
            out.write("{} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {}\n".format(params_cp.lsh_family, params_cp.distance_function, params_cp.l, params_cp.k, params_cp.last_cp_dimension, number_of_probes, constructtime, querytime, finscore, fullCandidates, len(gt), len(dataset), indexm, querym))
            out.close()