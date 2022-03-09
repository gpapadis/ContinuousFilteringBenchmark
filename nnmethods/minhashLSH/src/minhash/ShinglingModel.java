package minhash;

import java.util.*;

public class ShinglingModel {

    final int N;
    
    // vector containing all unique n-grams
    final List<String> ngramsVector;
    // N-gram size

    /**
     * Initialize class by extracting all n-grams from the given strings
     * @param entitiesSTR input strings
     * @param n n-grams size
     */
    public ShinglingModel(List<String> entitiesSTR, int n){
        this.N = n;
        Set<String> uniqueNGrams = new HashSet<>();
        for (String entityStr: entitiesSTR){
            Set<String> entityNGrams = getNGrams(entityStr, this.N);
            uniqueNGrams.addAll(entityNGrams);
        }
        this.ngramsVector = new ArrayList(uniqueNGrams);
    }


    /**
     * extract n-grams
     * @param str string
     * @param n n-grams size
     * @return a set of n-grams
     */
    public Set<String> getNGrams(String str, int n) {
        Set<String> ngrams = new HashSet<>();
        for (int i = 0; i < str.length() - n + 1; i++) {
            ngrams.add(str.substring(i, i + n));
        }
        return ngrams;
    }

    /**
     * get n-grams alongside with their frequencies
     * @param str string
     * @param n n-grams size
     * @return a set of n-grams alongside with their frequencies
     */
    public Map<String, Integer> getCountedNGrams(String str, int n) {
        Map<String, Integer> ngrams = new HashMap<>();
        for (int i = 0; i < str.length() - n + 1; i++) {
            String ngram = str.substring(i, i + n);
            if (ngrams.containsKey(ngram))
                ngrams.put(ngram, ngrams.get(ngram) + 1);
            else
                ngrams.put(ngram, 1);
        }
        return ngrams;
    }

    /**
     * get shingling vector
     * @param entityStr input string
     * @return  shingling vector
     */
    public boolean[] getBooleanVector(String entityStr){
        Set<String> entityNGrams = getNGrams(entityStr, this.N);
        boolean[] vector = new boolean[this.ngramsVector.size()];
        for (int i = 0; i<this.ngramsVector.size(); i++){
            String ngram = ngramsVector.get(i);
            vector[i] = entityNGrams.contains(ngram);
        }
        return vector;
    }

    /**
     * get shingling vector with frequencies
     * @param entityStr input string
     * @return  shingling vector with frequencies
     */
    public int[] getIntegerVector(String entityStr){
        Map<String, Integer> entityNGrams = getCountedNGrams(entityStr, this.N);
        int[] vector = new int[this.ngramsVector.size()];
        for (int i = 0; i<this.ngramsVector.size(); i++){
            String ngram = ngramsVector.get(i);
            vector[i] = entityNGrams.getOrDefault(ngram, 0);
        }
        return vector;
    }

    public boolean[][] booleanVectorization(List<String> entities){
        boolean[][] vectors = new boolean[entities.size()][this.ngramsVector.size()];
        for(int i=0; i<entities.size(); i++){
            vectors[i] = getBooleanVector(entities.get(i));
        }
        return vectors;
    }

    public int[][] vectorization(List<String> entities){
        int[][] vectors = new int[entities.size()][this.ngramsVector.size()];
        for(int i=0; i<entities.size(); i++){
            vectors[i] = getIntegerVector(entities.get(i));
        }
        return vectors;
    }

    public List<String> getNgramsVector() {
        return ngramsVector;
    }

    public int getVectorSize(){
        return ngramsVector.size();
    }

    public void shuffle(){
        Collections.shuffle(ngramsVector);
    }


    public int computeLength(boolean[] vector){
        int length = 0;
       for (boolean b : vector)
            if (b)
                length++;
        return length;
    }
}
