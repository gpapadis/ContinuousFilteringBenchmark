package joins;

import utilities.Tokenizer;
import utilities.Pair;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import utilities.RepresentationModel;

/**
 *
 * @author Georgios
 */
public class DefaultTopKSchemaAgnosticJoin extends AbstractJoin {

    public static void main(String[] args) {
        int K = 5;
        boolean[] reversed = {true, false, true, false, true, true, true, true, true, true};
        
        String mainDir = "/home/gap2/Documents/blockingNN/data/preprocessedSA/";

        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};
        String[] datasetsD1Rv = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] datasetsD2Rv = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
//        SimilarityFunction simFunction = SimilarityFunction.COSINE_SIM;
        Tokenizer tokenizer = Tokenizer.CHARACTER_FIVEGRAMS_MULTISET;

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\nCurrent dataset\t:\t" + datasetId);
            // read source entities
            String sPath = reversed[datasetId] ? datasetsD1Rv[datasetId] : datasetsD1[datasetId];
            String sourcePath = mainDir + sPath;
            EntitySerializationReader reader = new EntitySerializationReader(sourcePath);
            List<EntityProfile> sourceEntities = reader.getEntityProfiles();
            System.out.println("Source Entities: " + sourceEntities.size());

            // read target entities
            String tPath = reversed[datasetId] ? datasetsD2Rv[datasetId] : datasetsD2[datasetId];
            String targetPath = mainDir + tPath;
            reader = new EntitySerializationReader(targetPath);
            List<EntityProfile> targetEntities = reader.getEntityProfiles();
            System.out.println("Target Entities: " + targetEntities.size());

            // read ground-truth file
            String groundTruthPath = mainDir + groundtruthDirs[datasetId];
            GtSerializationReader gtReader = new GtSerializationReader(groundTruthPath);
            Set<IdDuplicates> gtDuplicates = gtReader.getDuplicatePairs(sourceEntities, targetEntities);
            System.out.println("GT Duplicates Entities: " + gtDuplicates.size());
            System.out.println();

            double averageIndexingTime = 0;
            double averageQueryingTime = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                int noOfEntities = sourceEntities.size();
                SOURCE_FREQUENCY = new int[noOfEntities];
                final Map<String, TIntList> index = indexSource(tokenizer, sourceEntities);

                int[] counters = new int[noOfEntities];
                int[] flags = new int[noOfEntities];
                for (int i = 0; i < noOfEntities; i++) {
                    flags[i] = -1;
                }
                
                long time2 = System.currentTimeMillis();

                int noOfTargetEntities = targetEntities.size();
                final List<Pair> candidatePairs = new ArrayList<>();
                for (int targetId = 0; targetId < noOfTargetEntities; targetId++) {
                    final String query = RepresentationModel.getAttributeValue(targetEntities.get(targetId));
                    final Set<String> tokens = RepresentationModel.tokenizeEntity(query, tokenizer);

                    final TIntSet candidates = new TIntHashSet();
                    for (String token : tokens) {
                        final TIntList sourceEnts = index.get(token);
                        if (sourceEnts == null) {
                            continue;
                        }

                        for (TIntIterator tIterator = sourceEnts.iterator(); tIterator.hasNext();) {
                            int sourceId = tIterator.next();
                            candidates.add(sourceId);
                            if (flags[sourceId] != targetId) {
                                counters[sourceId] = 0;
                                flags[sourceId] = targetId;
                            }
                            counters[sourceId]++;
                        }
                    }

                    if (candidates.isEmpty()) {
                        continue;
                    }

                    float minimumWeight = 0;
                    final PriorityQueue<Float> pq = new PriorityQueue<>();
                    for (TIntIterator tIterator = candidates.iterator(); tIterator.hasNext();) {
                        int sourceId = tIterator.next();
                        float commonTokens = counters[sourceId];

                        float sim = commonTokens / (float) Math.sqrt(((float) SOURCE_FREQUENCY[sourceId]) * tokens.size());
                        if (minimumWeight < sim) {
                            pq.add(sim);
                            if (K < pq.size()) {
                                minimumWeight = pq.poll();
                            }
                        }
                    }

                    minimumWeight = pq.poll();
                    for (TIntIterator tIterator = candidates.iterator(); tIterator.hasNext();) {
                        int sourceId = tIterator.next();
                        float commonTokens = counters[sourceId];

                        float sim = commonTokens / (float) Math.sqrt(((float) SOURCE_FREQUENCY[sourceId]) * tokens.size());
                        if (minimumWeight <= sim) {
                            if (reversed[datasetId]) {
                                candidatePairs.add(new Pair(targetId, sourceId));
                            } else {
                                candidatePairs.add(new Pair(sourceId, targetId));
                            }
                        }
                    }
                }
                long time3 = System.currentTimeMillis();
                averageIndexingTime += time2 - time1;
                averageQueryingTime += time3 - time2;

                if (iteration == 0) {
                    // true positive
                    long tp_ = 0;
                    // total verifications
                    long verifications_ = 0;
                    for (Pair p : candidatePairs) {
                        IdDuplicates pair = new IdDuplicates(p.getEntityId1(), p.getEntityId2());
                        if (gtDuplicates.contains(pair)) {
                            tp_ += 1;
                        }
                        verifications_ += 1;
                    }
                    float recall_ = (float) tp_ / (float) gtDuplicates.size();
                    float precision_ = (float) tp_ / (float) verifications_;
                    float f1_ = 2 * ((precision_ * recall_) / (precision_ + recall_));
                    System.out.println("Recall\t:\t" + recall_);
                    System.out.println("Precision\t:\t" + precision_);
                    System.out.println("F-Measure\t:\t" + f1_);
                }
                System.out.println("Candidates\t:\t" + candidatePairs.size());
            }
            System.out.println("Average indexing run-time\t:\t" + averageIndexingTime / ITERATIONS);
            System.out.println("Average querying run-time\t:\t" + averageQueryingTime / ITERATIONS);
        }
    }
}
