package joins;

import utilities.Tokenizer;
import utilities.SimilarityFunction;
import utilities.Pair;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class SchemaBasedEJoin extends AbstractJoin {

    public static void main(String[] args) {
        String[] mainDirs = {"/home/gap2/Documents/blockingNN/data/schemaBased/",
            "/home/gap2/Documents/blockingNN/data/preprocessedSB/"
        };

        boolean[] preprocessing = {true, true, false, false, false, false};
        float[] threshold = {0.63f, 0.38f, 0.39f, 1.00f, 0.15f, 0.74f};
        String[] attributes = {"Name", "Name", "Title", "Title", "Title", "Title"};
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "walmartProfiles", "dblpProfiles2"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "amazonProfiles2", "scholarProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "amazonWalmartIdDuplicates", "dblpScholarIdDuplicates"};
        SimilarityFunction[] simFunction = {SimilarityFunction.COSINE_SIM, SimilarityFunction.COSINE_SIM, SimilarityFunction.COSINE_SIM, SimilarityFunction.COSINE_SIM,
            SimilarityFunction.COSINE_SIM, SimilarityFunction.COSINE_SIM};
        Tokenizer[] tokenizer = {Tokenizer.CHARACTER_FOURGRAMS, Tokenizer.CHARACTER_TRIGRAMS_MULTISET, Tokenizer.CHARACTER_TRIGRAMS, Tokenizer.WHITESPACE, Tokenizer.CHARACTER_TRIGRAMS, Tokenizer.CHARACTER_TRIGRAMS_MULTISET};

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\nCurrent dataset\t:\t" + datasetId);

            // read source entities
            int dirId = preprocessing[datasetId] ? 1 : 0;
            String sourcePath = mainDirs[dirId] + datasetsD1[datasetId] + "_" + attributes[datasetId];
            EntitySerializationReader reader = new EntitySerializationReader(sourcePath);
            List<EntityProfile> sourceEntities = reader.getEntityProfiles();
            System.out.println("Source Entities: " + sourceEntities.size());

            // read target entities
            String targetPath = mainDirs[dirId] + datasetsD2[datasetId] + "_" + attributes[datasetId];
            reader = new EntitySerializationReader(targetPath);
            List<EntityProfile> targetEntities = reader.getEntityProfiles();
            System.out.println("Target Entities: " + targetEntities.size());

            // read ground-truth file
            String groundTruthPath = mainDirs[dirId] + groundtruthDirs[datasetId];
            GtSerializationReader gtReader = new GtSerializationReader(groundTruthPath);
            Set<IdDuplicates> gtDuplicates = gtReader.getDuplicatePairs(sourceEntities, targetEntities);
            System.out.println("GT Duplicates Entities: " + gtDuplicates.size());
            System.out.println();

            // first run
            int noOfEntities = sourceEntities.size();
            SOURCE_FREQUENCY = new int[noOfEntities];
            Map<String, TIntList> index = indexSource(tokenizer[datasetId], sourceEntities);

            int[] counters = new int[noOfEntities];
            int[] flags = new int[noOfEntities];
            for (int i = 0; i < noOfEntities; i++) {
                flags[i] = -1;
            }

            int targetId = 0;
            List<Pair> sims = new ArrayList<>(noOfEntities * targetEntities.size());
            for (EntityProfile e : targetEntities) {
                String query = RepresentationModel.getAttributeValue(e);
                Set<String> tokens = RepresentationModel.tokenizeEntity(query, tokenizer[datasetId]);

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

                for (TIntIterator tIterator = candidates.iterator(); tIterator.hasNext();) {
                    int sourceId = tIterator.next();
                    float commonTokens = counters[sourceId];

                    float sim = 0;
                    switch (simFunction[datasetId]) {
                        case COSINE_SIM:
                            sim = commonTokens / (float) Math.sqrt(((float) SOURCE_FREQUENCY[sourceId]) * tokens.size());
                            break;
                        case DICE_SIM:
                            sim = 2 * commonTokens / (SOURCE_FREQUENCY[sourceId] + tokens.size());
                            break;
                        case JACCARD_SIM:
                            sim = commonTokens / (SOURCE_FREQUENCY[sourceId] + tokens.size() - commonTokens);
                            break;
                    }

                    if (threshold[datasetId] <= sim) {
                        sims.add(new Pair(sourceId, targetId));
                    }
                }
                targetId++;
            }

            double duplicates = 0;
            for (Pair jp : sims) {
                if (gtDuplicates.contains(new IdDuplicates(jp.getEntityId1(), jp.getEntityId2()))) {
                    duplicates++;
                }
            }
            System.out.println("Candidates\t:\t" + sims.size());
            System.out.println("Duplicates\t:\t" + duplicates);

            // run-time measurement
            double averageIndexingTime = 0;
            double averageQueryingTime = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                noOfEntities = sourceEntities.size();
                SOURCE_FREQUENCY = new int[noOfEntities];
                index = indexSource(tokenizer[datasetId], sourceEntities);

                counters = new int[noOfEntities];
                flags = new int[noOfEntities];
                for (int i = 0; i < noOfEntities; i++) {
                    flags[i] = -1;
                }

                long time2 = System.currentTimeMillis();
                
                targetId = 0;
                sims = new ArrayList<>(noOfEntities * targetEntities.size());
                for (EntityProfile e : targetEntities) {
                    String query = RepresentationModel.getAttributeValue(e);
                    Set<String> tokens = RepresentationModel.tokenizeEntity(query, tokenizer[datasetId]);

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

                    for (TIntIterator tIterator = candidates.iterator(); tIterator.hasNext();) {
                        int sourceId = tIterator.next();
                        float commonTokens = counters[sourceId];

                        float sim = 0;
                        switch (simFunction[datasetId]) {
                            case COSINE_SIM:
                                sim = commonTokens / (float) Math.sqrt(((float) SOURCE_FREQUENCY[sourceId]) * tokens.size());
                                break;
                            case DICE_SIM:
                                sim = 2 * commonTokens / (SOURCE_FREQUENCY[sourceId] + tokens.size());
                                break;
                            case JACCARD_SIM:
                                sim = commonTokens / (SOURCE_FREQUENCY[sourceId] + tokens.size() - commonTokens);
                                break;
                        }

                        if (threshold[datasetId] <= sim) {
                            sims.add(new Pair(sourceId, targetId));
                        }
                    }
                    targetId++;
                }

                long time3 = System.currentTimeMillis();
                averageIndexingTime += time2 - time1;
                averageQueryingTime += time3 - time2;
            }
            System.out.println("Average indexing run-time\t:\t" + averageIndexingTime / ITERATIONS);
            System.out.println("Average querying run-time\t:\t" + averageQueryingTime / ITERATIONS);
        }
    }
}
