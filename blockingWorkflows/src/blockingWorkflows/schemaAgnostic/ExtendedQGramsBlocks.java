package blockingWorkflows.schemaAgnostic;

import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.ExtendedQGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
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

        boolean[] blockPurging = {false, false, false, true, true, false, false, false, true, false};

        int[] qConfId = new int[]{6, 8, 5, 1, 14, 14, 12, 12, 14, 5};
        int[] bfConfId = new int[]{0, 35, 19, 0, 30, 26, 6, 6, 18, 29};
        int[] mbConfId = new int[]{2, 5, 0, 4, 0, 0, 4, 0, 2, 5};
        ComparisonCleaningMethod[] mbAlg = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING, ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.WEIGHTED_NODE_PRUNING, ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING, ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING, ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING, ComparisonCleaningMethod.BLAST};

        String mainDir = "data/";
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        System.out.println("Main directory\t:\t" + mainDir);

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2[datasetId]);
            List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final ExtendedQGramsBlocking eqb = new ExtendedQGramsBlocking();
            eqb.setNumberedGridConfiguration(qConfId[datasetId]);
            System.out.println(eqb.getMethodConfiguration());

            List<AbstractBlock> blocks = eqb.getBlocks(profiles1, profiles2);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(true);
            if (blockPurging[datasetId]) {
                blocks = cbbp.refineBlocks(blocks);
            }

            final BlockFiltering bf = new BlockFiltering();
            bf.setNumberedGridConfiguration(bfConfId[datasetId]);
            System.out.println(bf.getMethodConfiguration());

            List<AbstractBlock> bfBlocks = bf.refineBlocks(blocks);

            final IBlockProcessing bpMethod = ComparisonCleaningMethod.getDefaultConfiguration(mbAlg[datasetId]);
            bpMethod.setNumberedGridConfiguration(mbConfId[datasetId]);
            System.out.println("MB=" + bpMethod.getMethodName());
            System.out.println(bpMethod.getMethodConfiguration());

            List<AbstractBlock> mbBlocks = bpMethod.refineBlocks(bfBlocks);

            final BlocksPerformance blStats = new BlocksPerformance(mbBlocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageBf = 0;
            double averageBp = 0;
            double averageBu = 0;
            double averageMb = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = eqb.getBlocks(profiles1, profiles2);

                long time2 = System.currentTimeMillis();

                if (blockPurging[datasetId]) {
                    blocks = cbbp.refineBlocks(blocks);
                }

                long time3 = System.currentTimeMillis();

                bfBlocks = bf.refineBlocks(blocks);

                long time4 = System.currentTimeMillis();

                bpMethod.refineBlocks(bfBlocks);

                long time5 = System.currentTimeMillis();

                averageBp += time3 - time2;
                averageBf += time4 - time3;
                averageBu += time2 - time1;
                averageMb += time5 - time4;
            }
            averageBf /= ITERATIONS;
            averageBp /= ITERATIONS;
            averageBu /= ITERATIONS;
            averageMb /= ITERATIONS;
            System.out.println("Average block building run-time\t:\t" + averageBu);
            System.out.println("Average block filtering run-time\t:\t" + averageBf);
            System.out.println("Average block purging run-time\t:\t" + averageBp);
            System.out.println("Average meta-blocking run-time\t:\t" + averageMb);
        }
    }
}
