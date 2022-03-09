package minhash;

import info.debatty.java.lsh.LSH;

import java.util.HashSet;
import java.util.Set;

public abstract class LocalitySensitiveHashing {
    LSH lsh;

    // rows per band
    int r = 5;

    // size of vector
    int vectorSize = 2048;

    // number of bands
    int bands = (int) Math.ceil((float) vectorSize /(float) r);

    // number of buckets
    int numOfBuckets = 50;

    // an array of bands of buckets containing a set of entity IDs
    Set<Integer>[][] buckets;


    /**
     * given an entity compute its hash, i.e., the buckets it belongs to
     * @param vector model
     * @return the indices of buckets
     */
    public abstract int[] hash(float[] vector);


    /**
     * Index a list of entities into the buckets
     *
     * @param vectors a list of models
     */
    public void index(float[][] vectors){
        this.buckets = (HashSet<Integer>[][]) new HashSet[this.bands][this.numOfBuckets];
        for (int i=0; i<vectors.length; i++){
            if (nonEmpty(vectors[i])) {
                int[] hashes = hash(vectors[i]);
                for (int b = 0; b < hashes.length; b++) {
                    int hash = hashes[b];
                    if (buckets[b][hash] == null) {
                        HashSet<Integer> bucketEntities = new HashSet<>();
                        buckets[b][hash] = bucketEntities;
                    }
                    Set<Integer> bucketEntities = buckets[b][hash];
                    bucketEntities.add(i);
                }
            }
        }
    }


    /**e
     * find the candidates of an entity.
     * @param vector target model
     * @return a set of the IDs of the candidate entities
     */
    public Set<Integer> query(float[] vector){
        Set<Integer> candidates = new HashSet<>();
        if (nonEmpty(vector)) {
            int[] hashes = hash(vector);
            for (int b = 0; b < hashes.length; b++) {
                int hash = hashes[b];
                if (buckets[b][hash] != null)
                    candidates.addAll(buckets[b][hash]);
            }
        }
        return candidates;
    }

    private boolean nonEmpty(float[] vector){
        for (int i=0; i<vectorSize; i++){
            if (vector[i] > 0)
                return true;
        }
        return false;
    }
}