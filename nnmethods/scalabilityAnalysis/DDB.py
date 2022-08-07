#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from datetime import datetime
import pandas as pd
from deep_blocker import DeepBlocker
from tuple_embedding_models import  AutoEncoderTupleEmbedding
from vector_pairing_models import ExactTopKVectorPairing
import blocking_utils

deli = '|'
k = 5
column = 'Clean Ag.Value'
main_dir = '/home/data/'
for size in ['10K', '50K', '100K', '200K', '300K', '1M', '2M']:  #which dataset should be used
    sys.stdout = open(main_dir +"Processed/Output/DDB"+size+".txt", 'w')
    left_df = pd.read_csv(main_dir + size +"full.csv", sep=deli)
    left_df = left_df[['Id', column]]
    left_df.columns = ['id', column]
    #right_df = pd.read_csv(main_dir +"Datasets/"+ "D1Bemb.csv", sep=deli)
    golden_df = pd.read_csv(main_dir +size+ "duplicates.csv", sep=deli)
    
    l_id = 'ltable_id'
    r_id = 'rtable_id'
    golden_df.columns = ['ltable_id', 'rtable_id']
    
    corr_sorted = golden_df[golden_df[l_id]<golden_df[r_id]]
    inv_sorted = golden_df[golden_df[l_id]>golden_df[r_id]]
    inv_sorted = inv_sorted.reindex([r_id, l_id], axis=1)
    inv_sorted.columns = [l_id, r_id]
    golden_df = pd.merge(corr_sorted, inv_sorted, on=[l_id, r_id], how='outer')
    del corr_sorted
    del inv_sorted
   
    cols_to_block = [column]
   
    for iteration in (0,10): 
        time_1 = datetime.now()
       
        tuple_embedding_model = AutoEncoderTupleEmbedding()
        topK_vector_pairing_model = ExactTopKVectorPairing(K=k)
        db = DeepBlocker(tuple_embedding_model, topK_vector_pairing_model)
        candidate_set_df = db.block_datasets(left_df, left_df, cols_to_block)
        
        time_2 = datetime.now()
        time_diff = (time_2 - time_1)
        print("Run-time", time_diff.total_seconds() * 1000)
        
        print(len(candidate_set_df[candidate_set_df[l_id]>candidate_set_df[r_id]]))
        
        results = blocking_utils.compute_blocking_statistics(candidate_set_df, golden_df, left_df, left_df)
        rc = results.get("recall")
        
        print("Recall", results.get("recall"))
        print("Precision", results.get("precision"))
        print("Candidates", results.get("candidates"))
