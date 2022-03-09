package experiments;

import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;

import java.util.*;
import minhash.LocalitySensitiveHashing;
import minhash.MinHash;
import minhash.Pair;
import minhash.Reader;
import minhash.ShinglingModel;
import minhash.Utilities;

public class schemaBased {

    static int ITERATIONS = 10;

    public static void main(String[] args) {
        boolean[] preprocessed = {true, true, false, true, true, false};

        int[] bands = {4, 32, 32, 2, 32, 2};
        int[] buckets = {128, 16, 16, 256, 16, 256};
        int[] k = {2, 2, 2, 5, 2, 5};

        String[] mainDirs = {"/home/gap2/Documents/blockingNN/data/schemaBased/",
            "/home/gap2/Documents/blockingNN/data/preprocessedSB/"
        };

        String[] attributes = {"Name", "Name", "Title", "Title", "Title", "Title"};
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "walmartProfiles", "dblpProfiles2"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "amazonProfiles2", "scholarProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates"/*, "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates"*/, "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates"/*, "moviesIdDuplicates"*/};

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            // read source entities
            int dirId = preprocessed[datasetId] ? 1 : 0;
            String sourcePath = mainDirs[dirId] + datasetsD1[datasetId] + "_" + attributes[datasetId];
            List<EntityProfile> sourceEntities = Reader.readSerialized(sourcePath);
            System.out.println("Source Entities: " + sourceEntities.size());

            // read target entities
            String targetPath = mainDirs[dirId] + datasetsD2[datasetId] + "_" + attributes[datasetId];
            List<EntityProfile> targetEntities = Reader.readSerialized(targetPath);
            System.out.println("Target Entities: " + targetEntities.size());

            // read ground-truth file
            String groundTruthPath = mainDirs[dirId] + groundtruthDirs[datasetId];
            Set<IdDuplicates> gtDuplicates = Reader.readSerializedGT(groundTruthPath, sourceEntities, targetEntities);
            System.out.println("GT Duplicates Entities: " + gtDuplicates.size());
            System.out.println();

            double averageIndexingTime = 0;
            double averageQueryingTime = 0;
            double averageRecall = 0;
            double averagePrecision = 0;
            double averageCandidates = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                List<String> sourceSTR = Utilities.entities2String(sourceEntities);

                ShinglingModel model = new ShinglingModel(sourceSTR, k[datasetId]);
                int[][] sourceVectorsInt = model.vectorization(sourceSTR);
                float[][] sVectors = new float[sourceVectorsInt.length][];
                for (int row = 0; row < sourceVectorsInt.length; row++) {
                    double[] tempArray  = Arrays.stream(sourceVectorsInt[row]).asDoubleStream().toArray();
                    sVectors[row] = new float[tempArray.length];
                    for (int i = 0; i < tempArray.length; i++) {
                        sVectors[row][i] = (float) tempArray[i];
                    }
                }

                // initialize LSH
                LocalitySensitiveHashing lsh = new MinHash(sVectors, bands[datasetId], buckets[datasetId], model.getVectorSize());

                long time2 = System.currentTimeMillis();

                List<String> targetSTR = Utilities.entities2String(targetEntities);

                int[][] targetVectorsInt = model.vectorization(targetSTR);
                float[][] tVectors = new float[targetVectorsInt.length][];
                for (int row = 0; row < targetVectorsInt.length; row++) {
                    double[] tempArray = Arrays.stream(targetVectorsInt[row]).asDoubleStream().toArray();
                    tVectors[row] = new float[tempArray.length];
                    for (int i = 0; i < tempArray.length; i++) {
                        tVectors[row][i] = (float) tempArray[i];
                    }
                }

                // for each target entity, find its candidates (query)
                // find TP by searching the pairs in GT
                final List<Pair> candidatePairs = new ArrayList<>();
                for (int j = 0; j < targetEntities.size(); j++) {
                    float[] vector = tVectors[j];
                    Set<Integer> candidates = lsh.query(vector);
                    for (Integer c : candidates) {
                        candidatePairs.add(new Pair(c, j));
                    }
                }
                long time3 = System.currentTimeMillis();

                averageIndexingTime += time2 - time1;
                averageQueryingTime += time3 - time2;

                // true positive
                long tp_ = 0;
                // total verifications
                long verifications_ = 0;
                for (int j = 0; j < targetEntities.size(); j++) {
                    float[] vector = tVectors[j];
                    Set<Integer> candidates = lsh.query(vector);
                    for (Integer c : candidates) {
                        IdDuplicates pair = new IdDuplicates(c, j);
                        if (gtDuplicates.contains(pair)) {
                            tp_ += 1;
                        }
                        verifications_ += 1;
                    }
                }
                float recall_ = (float) tp_ / (float) gtDuplicates.size();
                float precision_ = (float) tp_ / (float) verifications_;
                averageRecall += recall_;
                averagePrecision += precision_;
                averageCandidates += candidatePairs.size();
            }
            System.out.println("Average indexing time\t:\t" + averageIndexingTime / ITERATIONS);
            System.out.println("Average querying time\t:\t" + averageQueryingTime / ITERATIONS);
            System.out.println("Recall\t:\t" + averageRecall / ITERATIONS);
            System.out.println("Precision\t:\t" + averagePrecision / ITERATIONS);
            System.out.println("Candidates\t:\t" + averageCandidates / ITERATIONS);
        }
    }
}
