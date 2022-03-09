import time
import pandas as pd
from deep_blocker import DeepBlocker
from tuple_embedding_models import  AutoEncoderTupleEmbedding, CTTTupleEmbedding, HybridTupleEmbedding
from vector_pairing_models import ExactTopKVectorPairing
import blocking_utils

deli = '#'
main_dir = "/home/gap2/Documents/blockingNN/data/"
left_df = pd.read_csv(main_dir + "D3Bemb.csv", sep=deli)
left_df = left_df[['Id', 'Clean Ag.Value']]
left_df.columns = ['id', 'Clean Ag.Value']
right_df = pd.read_csv(main_dir + "D3Aemb.csv", sep=deli)
right_df = right_df[['Id', 'Clean Ag.Value']]
right_df.columns = ['id', 'Clean Ag.Value']
golden_df = pd.read_csv(main_dir + "D3groundtruthRev.csv", sep=deli)
golden_df.columns = ['ltable_id', 'rtable_id']
cols_to_block = ["Clean Ag.Value"]

k = 5

tuple_embedding_model = AutoEncoderTupleEmbedding()
topK_vector_pairing_model = ExactTopKVectorPairing(K=k)
db = DeepBlocker(tuple_embedding_model, topK_vector_pairing_model)
candidate_set_df = db.block_datasets(left_df, right_df, cols_to_block)

results = blocking_utils.compute_blocking_statistics(candidate_set_df, golden_df, left_df, right_df)
print("Recall", results.get("recall"))
print("Precision", results.get("pq"))
print("Candidates", results.get("candidates"))
