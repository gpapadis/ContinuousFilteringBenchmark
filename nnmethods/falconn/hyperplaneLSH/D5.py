from __future__ import print_function
import numpy as np
import falconn
import timeit
import math
import psutil

if __name__ == '__main__':
    datasetN = 5
    column = 'Embedded Title'
    datapart = 'A'
    querypart = 'B'
    main_dir = "/home/gap2/Documents/blockingNN/data/csvProfiles/falconn/"

    for column in ['Embedded Clean Name', 'Embedded Ag.Value']:
        for iteration in range(0, 10):
            print('Reading the dataset')
            dataset = np.load(main_dir + 'D{}'.format(datasetN)+datapart+'-'+column+'.npy')
            queries = np.load(main_dir + 'D{}'.format(datasetN)+querypart+'-'+column+'.npy')
            IDdata = list(np.load(main_dir + 'D{}'.format(datasetN)+datapart+'ID-'+column+'.npy'))
            IDqueries = list(np.load(main_dir + 'D{}'.format(datasetN)+querypart+'ID-'+column+'.npy'))
            gt = np.load(main_dir + 'D{}'.format(datasetN)+'GT-'+column+'.npy')
            print('Done')

            # It's important not to use doubles, unless they are strictly necessary.
            # If your dataset consists of doubles, convert it to floats using `astype`.
            assert dataset.dtype == np.float32

            # Normalize all the lenghts, since we care about the cosine similarity.
            print('Normalizing the dataset')
            dataset /= np.linalg.norm(dataset, axis=1).reshape(-1, 1)
            queries /= np.linalg.norm(queries, axis=1).reshape(-1, 1)
            print('Done')

            # Perform linear scan using NumPy to get answers to the queries.
            #print('Solving queries using linear scan')
            #t1 = timeit.default_timer()
            #answers = []
            #for query in queries:
            #    answers.append(np.dot(dataset, query).argmax())
            #t2 = timeit.default_timer()
            #print('Done')
            #print('Linear scan time: {} per query'.format((t2 - t1) / float(
            #   len(queries))))

            # Center the dataset and the queries: this improves the performance of LSH quite a bit.
            print('Centering the dataset and queries')
            center = np.mean(dataset, axis=0)
            dataset -= center
            queries -= center
            print('Done')

            #params_cp = falconn.get_default_parameters(len(dataset), len(dataset[0]), falconn.DistanceFunction.EuclideanSquared, True)
            #print('lsh family: ', params_cp.lsh_family)
            #print('number of tables: ', params_cp.l)
            #print('number of rotations: ', params_cp.num_rotations)
            #print('number of hash functions: ', params_cp.k)

            # we build only 50 tables, increasing this quantity will improve the query time
            # at a cost of slower preprocessing and larger memory footprint, feel free to
            # play with this number
            outstring = main_dir + "Output/D{}_HP".format(datasetN)+column+datapart+querypart+".txt"
            out = open(outstring, 'a')
            out.write("LSH \t dist Funct \t num tables \t num hash funct \t num probes \t index time \t query time \t true matches \t all candidates \t all matches \t Index \t Query \t Indexmemory \t Searchmemory \t storage\n")
            out.close()

            if column == "Embedded Clean Name":
                number_of_tables = 5
                number_of_functions = 25
                number_of_probes = 5
                storage = falconn.StorageHashTable.LinearProbingHashTable

            if column == "Embedded Ag.Value":
                number_of_tables = 17
                number_of_functions = 12
                number_of_probes = 461
                storage = falconn.StorageHashTable.BitPackedFlatHashTable
            
            t1 = timeit.default_timer()
            #params_cp = falconn.get_default_parameters(len(dataset)+len(queries), len(dataset[0]), falconn.DistanceFunction.EuclideanSquared,False)
            params_cp = falconn.LSHConstructionParameters()
            params_cp.dimension = len(dataset[0])
            params_cp.lsh_family = falconn.LSHFamily.Hyperplane
            params_cp.distance_function = falconn.DistanceFunction.NegativeInnerProduct #EuclideanSquared #
            params_cp.l = number_of_tables
            # we set one rotation, since the data is dense enough,
            # for sparse data set it to 2
            params_cp.num_rotations = 1
            params_cp.seed = 5721840
            # we want to use all the available threads to set up
            params_cp.num_setup_threads = 0
            params_cp.storage_hash_table = storage
            # we build 18-bit hashes so that each table has
            # 2^18 bins; this is a good choise since 2^18 is of the same
            # order of magnitude as the number of data points
            #falconn.compute_number_of_hash_functions(num_hash_bits, params_cp)
            params_cp.k = number_of_functions

            print('Constructing the LSH table')
            m1= psutil.Process().memory_info().rss / (1024 * 1024)
            table = falconn.LSHIndex(params_cp)
            table.setup(dataset)
            m2 = psutil.Process().memory_info().rss / (1024 * 1024)
            t2 = timeit.default_timer()
            print('Done')
            constructtime = t2-t1
            indexm = m2-m1
            print('Construction time: {}'.format(constructtime))

            t1 = timeit.default_timer()
            query_object = table.construct_query_object()


            # find the smallest number of probes to achieve accuracy 0.9
            # using the binary search
            print('Choosing number of probes')
            #number_of_probes = params_cp.l

            def evaluate_number_of_probes(number_of_probes):
                query_object.set_num_probes(number_of_probes)
                score = 0
                
                for i,j in gt:
                  if IDdata.index(i) in query_object.get_unique_candidates(queries[IDqueries.index(j)]):
                      score +=1
                #for (i, query) in enumerate(queries):
                #    if answers[i] in query_object.get_candidates_with_duplicates(
                #            query):
                #        score += 1
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

            m1 = psutil.Process().memory_info().rss / (1024 * 1024)
            finscore = 0
            for i,j in gt:
                if IDdata.index(i) in query_object.get_unique_candidates(queries[IDqueries.index(j)]):
                  finscore +=1

            m2= psutil.Process().memory_info().rss / (1024 * 1024)
            t2 = timeit.default_timer()
            querym = m2-m1
            querytime = t2-t1
            print('Query time: {}'.format((querytime) / len(queries)))
            print('Precision: {}'.format(float(finscore) / len(gt)))

            #full number of candidates:
            fullCandidates=0
            for query in queries:
                fullCandidates += len(query_object.get_unique_candidates(query))
            #out.write("{} \t {}".format(num_hash_bits, number_of_tables))
            out = open(outstring, 'a')
            out.write("{} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \t {} \n".format(params_cp.lsh_family, params_cp.distance_function, params_cp.l, params_cp.k, number_of_probes, constructtime, querytime, finscore, fullCandidates, len(gt), len(dataset), len(queries), indexm, querym, params_cp.storage_hash_table))
            out.close()
