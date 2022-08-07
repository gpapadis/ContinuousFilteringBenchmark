package scalabilityAnalysis;

import utilities.Tokenizer;
import utilities.Pair;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import joins.AbstractJoin;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import utilities.RepresentationModel;

/**
 *
 * @author Georgios
 */
public class SchemaAgnosticEJoin extends AbstractJoin {

    public static void main(String[] args) {
        String mainDir = "/home/data/syntheticData/";
        String[] datasets = {"10K", "50K", "100K", "200K", "300K", "1M", "2M"};

        for (int datasetId = 0; datasetId < datasets.length; datasetId++) {
            System.out.println("\n\nCurrent dataset\t:\t" + datasetId);

            // read source entities
            String sourcePath = mainDir + datasets[datasetId] + "profiles";
            EntitySerializationReader reader = new EntitySerializationReader(sourcePath);
            List<EntityProfile> sourceEntities = reader.getEntityProfiles();
            System.out.println("Source Entities: " + sourceEntities.size());

            // read ground-truth file
            String groundTruthPath = mainDir + datasets[datasetId] + "IdDuplicates";
            GtSerializationReader gtReader = new GtSerializationReader(groundTruthPath);
            Set<IdDuplicates> gtDuplicates = gtReader.getDuplicatePairs(sourceEntities);
            System.out.println("GT Duplicates Entities: " + gtDuplicates.size());
            System.out.println();

            float threshold = 0.44f;
            Tokenizer tokenizer = Tokenizer.CHARACTER_BIGRAMS;
//            SimilarityFunction simFunction = SimilarityFunction.JACCARD_SIM;

            for (int iterations = 0; iterations < ITERATIONS; iterations++) {
                long time1 = System.currentTimeMillis();

                // first run
                int noOfEntities = sourceEntities.size();
                SOURCE_FREQUENCY = new int[noOfEntities];
                final Map<String, TIntList> index = new HashMap<>();

                int[] counters = new int[noOfEntities];
                int[] flags = new int[noOfEntities];
                for (int i = 0; i < noOfEntities; i++) {
                    flags[i] = -1;
                }

                final List<Pair> sims = new ArrayList<>(noOfEntities * 1000);
                for (int id = 0; id < noOfEntities; id++) {
                    final String query = RepresentationModel.getAttributeValue(sourceEntities.get(id));
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
                            if (flags[sourceId] != id) {
                                counters[sourceId] = 0;
                                flags[sourceId] = id;
                            }
                            counters[sourceId]++;
                        }
                    }

                    for (String token : tokens) {
                        TIntList ids = index.get(token);
                        if (ids == null) {
                            ids = new TIntArrayList();
                            index.put(token, ids);
                        }
                        ids.add(id);
                    }
                    SOURCE_FREQUENCY[id] = tokens.size();

                    if (candidates.isEmpty()) {
                        continue;
                    }

                    for (TIntIterator tIterator = candidates.iterator(); tIterator.hasNext();) {
                        int sourceId = tIterator.next();
                        float commonTokens = counters[sourceId];
                        float sim = commonTokens / (SOURCE_FREQUENCY[sourceId] + tokens.size() - commonTokens);
                        if (threshold <= sim) {
                            sims.add(new Pair(id, sourceId));
                        }
                    }
                }

                long time2 = System.currentTimeMillis();
                System.out.println("Run-time\t:\t" + (time2 - time1));

                double duplicates = 0;
                for (Pair jp : sims) {
                    if (gtDuplicates.contains(new IdDuplicates(jp.getEntityId1(), jp.getEntityId2()))
                            || gtDuplicates.contains(new IdDuplicates(jp.getEntityId2(), jp.getEntityId1()))) {
                        duplicates++;
                    }
                }
                double recall_ = duplicates / gtDuplicates.size();
                double precision_ = duplicates / sims.size();
                double f1_ = 2 * ((precision_ * recall_) / (precision_ + recall_));
                System.out.println("Recall\t:\t" + recall_);
                System.out.println("Precision\t:\t" + precision_);
                System.out.println("F-Measure\t:\t" + f1_);
                System.out.println("Candidates\t:\t" + sims.size());
            }
        }
    }
}
