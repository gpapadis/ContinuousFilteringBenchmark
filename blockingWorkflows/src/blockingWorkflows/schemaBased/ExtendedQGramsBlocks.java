package blockingWorkflows.schemaBased;

import Utilities.*;
import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.ExtendedQGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.ComparisonCleaningMethod;

/**
 *
 * @author Georgios
 */
public class ExtendedQGramsBlocks {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        BasicConfigurator.configure();

        boolean[] blockFiltering = {true, true, true, true, true, true, false, true, true, true};
        boolean[] metablocking = {true, true, true, true, true, true, true, true, true, false};

        int[] qConfId = new int[]{5, 3, 8, 4, 0, 0, 0, 5, 14, 0};
        int[] bfConfId = new int[]{38, 12, 29, 0, 38, 12, 9, 29, 15, 39};
        int[] mbConfId = new int[]{1, 0, 5, 4, 0, 0, 0, 5, 2, 1};
        ComparisonCleaningMethod[] mbAlg = {ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_NODE_PRUNING, ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING, ComparisonCleaningMethod.CARDINALITY_EDGE_PRUNING,
            ComparisonCleaningMethod.CARDINALITY_EDGE_PRUNING, ComparisonCleaningMethod.CARDINALITY_EDGE_PRUNING,
            ComparisonCleaningMethod.BLAST, ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING};
        
        String mainDir = "data/";
        String[] attributesD1 = {"http://www.okkam.org/ontology_restaurant1.owl#name", "name", "title", "title", "https://www.scads.de/movieBenchmark/ontology/name", 
            "https://www.scads.de/movieBenchmark/ontology/name", "https://www.scads.de/movieBenchmark/ontology/title", "title", "title", "title"};
        String[] attributesD2 = {"http://www.okkam.org/ontology_restaurant2.owl#name", "name", "title", "title", "https://www.scads.de/movieBenchmark/ontology/name", 
            "https://www.scads.de/movieBenchmark/ontology/name", "https://www.scads.de/movieBenchmark/ontology/title", "title", "title", "title"};
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            profiles1 = ProfileProcessing.reduceProfilesToAttribute(profiles1, attributesD1[datasetId]);
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2[datasetId]);
            List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            profiles2 = ProfileProcessing.reduceProfilesToAttribute(profiles2, attributesD2[datasetId]);
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final ExtendedQGramsBlocking eqb = new ExtendedQGramsBlocking();
            eqb.setNumberedGridConfiguration(qConfId[datasetId]);
            System.out.println(eqb.getMethodConfiguration());

            List<AbstractBlock> blocks = eqb.getBlocks(profiles1, profiles2);

            final BlockFiltering bf = new BlockFiltering();
            if (blockFiltering[datasetId]) {
                bf.setNumberedGridConfiguration(bfConfId[datasetId]);
                System.out.println(bf.getMethodConfiguration());

                blocks = bf.refineBlocks(blocks);
            }

            final ComparisonPropagation cp = new ComparisonPropagation();
            final IBlockProcessing bpMethod = ComparisonCleaningMethod.getDefaultConfiguration(mbAlg[datasetId]);
            if (metablocking[datasetId]) {
                bpMethod.setNumberedGridConfiguration(mbConfId[datasetId]);
                System.out.println("MB=" + bpMethod.getMethodName());
                System.out.println(bpMethod.getMethodConfiguration());

                blocks = bpMethod.refineBlocks(blocks);
            } else {
                blocks = cp.refineBlocks(blocks);
            }

            final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageBf = 0;
            double averageBu = 0;
            double averageMb = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = eqb.getBlocks(profiles1, profiles2);
                
                long time2 = System.currentTimeMillis();
                
                if (blockFiltering[datasetId]) {
                    blocks = bf.refineBlocks(blocks);
                }
                
                long time3 = System.currentTimeMillis();
                
                if (metablocking[datasetId]) {
                    bpMethod.refineBlocks(blocks);
                } else {
                    blocks = cp.refineBlocks(blocks);
                }

                long time4 = System.currentTimeMillis();

                averageBf += time3 - time2;
                averageBu += time2 - time1;
                averageMb += time4 - time3;
            }
            averageBf /= ITERATIONS;
            averageBu /= ITERATIONS;
            averageMb /= ITERATIONS;
            System.out.println("Average block building run-time\t:\t" + averageBu);
            System.out.println("Average block filtering run-time\t:\t" + averageBf);
            System.out.println("Average meta-blocking run-time\t:\t" + averageMb);
        }
    }
}
