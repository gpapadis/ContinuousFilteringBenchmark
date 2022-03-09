import time
import pandas as pd
from deep_blocker import DeepBlocker
from tuple_embedding_models import  AutoEncoderTupleEmbedding, CTTTupleEmbedding, HybridTupleEmbedding
from vector_pairing_models import ExactTopKVectorPairing
import blocking_utils

deli = '|'
main_dir = "/home/gap2/Documents/blockingNN/data/"
left_df = pd.read_csv(main_dir + "D2Bemb.csv", sep=deli)
left_df = left_df[['Id', 'Clean Name']]
left_df.columns = ['id', 'Clean Name']
left_df['Clean Name'] = left_df['Clean Name'].astype(str)
right_df = pd.read_csv(main_dir + "D2Aemb.csv", sep=deli)
right_df = right_df[['Id', 'Clean Name']]
right_df.columns = ['id', 'Clean Name']
right_df['Clean Name'] = right_df['Clean Name'].astype(str)
golden_df = pd.read_csv(main_dir + "D2groundtruthRev.csv", sep=deli)
golden_df.columns = ['ltable_id', 'rtable_id']
cols_to_block = ["Clean Name"]

k = 5

tuple_embedding_model = AutoEncoderTupleEmbedding()
topK_vector_pairing_model = ExactTopKVectorPairing(K=k)
db = DeepBlocker(tuple_embedding_model, topK_vector_pairing_model)
candidate_set_df = db.block_datasets(left_df, right_df, cols_to_block)

results = blocking_utils.compute_blocking_statistics(candidate_set_df, golden_df, left_df, right_df)
print("Recall", results.get("recall"))
print("Precision", results.get("pq"))
print("Candidates", results.get("candidates"))
