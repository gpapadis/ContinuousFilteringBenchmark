package blockingWorkflows.schemaBased;

import Utilities.*;
import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.ExtendedSuffixArraysBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
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
public class ExtendedSuffixArrays {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        BasicConfigurator.configure();

        int[] bbConfId = new int[]{499, 85, 485, 99, 180, 450, 445, 454, 499, 498};
        int[] mbConfId = new int[]{1, 0, 3, 0, 2, 1, 4, 0, 2, 1};
        ComparisonCleaningMethod[] mbAlg = {ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.BLAST, ComparisonCleaningMethod.BLAST, ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.BLAST, ComparisonCleaningMethod.BLAST, ComparisonCleaningMethod.WEIGHTED_NODE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_NODE_PRUNING, ComparisonCleaningMethod.CARDINALITY_EDGE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_NODE_PRUNING};

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

            final ExtendedSuffixArraysBlocking sa = new ExtendedSuffixArraysBlocking();
            sa.setNumberedGridConfiguration(bbConfId[datasetId]);
            System.out.println(sa.getMethodConfiguration());

            List<AbstractBlock> blocks = sa.getBlocks(profiles1, profiles2);

            IBlockProcessing bpMethod = ComparisonCleaningMethod.getDefaultConfiguration(mbAlg[datasetId]);
            bpMethod.setNumberedGridConfiguration(mbConfId[datasetId]);
            System.out.println("MB=" + bpMethod.getMethodName());
            System.out.println(bpMethod.getMethodConfiguration());

            final List<AbstractBlock> mbBlocks = bpMethod.refineBlocks(blocks);

            final BlocksPerformance blStats = new BlocksPerformance(mbBlocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageBu = 0;
            double averageMb = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = sa.getBlocks(profiles1, profiles2);

                long time2 = System.currentTimeMillis();

                bpMethod.refineBlocks(blocks);

                long time3 = System.currentTimeMillis();

                averageBu += time2 - time1;
                averageMb += time3 - time2;
//                averageRt += rt;
//                System.out.println("Run-time\t:\t" + rt);
            }
            averageBu /= ITERATIONS;
            averageMb /= ITERATIONS;
            System.out.println("Average block building run-time\t:\t" + averageBu);
            System.out.println("Average meta-blocking run-time\t:\t" + averageMb);
        }
    }
}
