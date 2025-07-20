package l1.der;

import java.util.List;
import org.scify.jedai.blockbuilding.StandardBlocking;
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
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.ComparisonCleaningMethod;

/**
 *
 * @author G_A.Papadakis
 */
public class TokenBlocking {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        String mainDir = "/home/servers/conf/data/";
        String[] datasets = {"cddbProfiles", "coraProfiles", "cddbtitle", "coratitle"};
        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};
//        String[] datasets = {"cddbProfiles", "coraProfiles", "10Kaddress_1", "cddbtitle", "coratitle"};
//        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "10KIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};

//        int[] blockPurging = {-1, 1, -1, 1, -1};
//        int[] bfRatio = {3, 37, 33, 29, 37};
//        int[] wScheme = {0, 2, 0, 5, 6};
//        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
//            ComparisonCleaningMethod.BLAST,
//            ComparisonCleaningMethod.BLAST,
//            ComparisonCleaningMethod.CARDINALITY_EDGE_PRUNING,
//            ComparisonCleaningMethod.BLAST
//        };
        int[] blockPurging = {-1, 1, 1, -1};
        int[] bfRatio = {3, 37, 29, 37};
        int[] wScheme = {0, 2, 5, 6};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.CARDINALITY_EDGE_PRUNING,
            ComparisonCleaningMethod.BLAST
        };

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasets[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasets[datasetId]);
            List<EntityProfile> profiles = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final StandardBlocking sb = new StandardBlocking();
            List<AbstractBlock> blocks = sb.getBlocks(profiles);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(false);
            if (0 < blockPurging[datasetId]) {
                blocks = cbbp.refineBlocks(blocks);
                System.out.println("BP in");
            } else {
                System.out.println("BP out");
            }

            final BlockFiltering bf = new BlockFiltering();
            if (0 < bfRatio[datasetId]) {
                bf.setNumberedGridConfiguration(bfRatio[datasetId]);
                blocks = bf.refineBlocks(blocks);
            } else {
                System.out.println("BF out");
            }

            final IBlockProcessing bpMethod = ComparisonCleaningMethod.getDefaultConfiguration(mbAlgorithm[datasetId]);
            bpMethod.setNumberedGridConfiguration(wScheme[datasetId]);
            blocks = bpMethod.refineBlocks(blocks);

            final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageRt = 0;
            double averageBBPortion = 0;
            double averageBFPortion = 0;
            double averageBPPortion = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = sb.getBlocks(profiles);

                long time2 = System.currentTimeMillis();

                if (0 < blockPurging[datasetId]) {
                    blocks = cbbp.refineBlocks(blocks);
                }

                long time3 = System.currentTimeMillis();

                if (0 < bfRatio[datasetId]) {
                    blocks = bf.refineBlocks(blocks);
                }

                long time4 = System.currentTimeMillis();

                blocks = bpMethod.refineBlocks(blocks);

                long time5 = System.currentTimeMillis();

                double rt = time5 - time1;
                averageRt += rt;
                averageBBPortion += (time2 - time1) / rt;
                if (0 < blockPurging[datasetId]) {
                    averageBPPortion += (time3 - time2) / rt;
                }
                if (0 < bfRatio[datasetId]) {
                    averageBFPortion += (time4 - time3) / rt;
                }
                System.out.println("Run-time\t:\t" + rt);
            }
            averageRt /= ITERATIONS;
            averageBBPortion /= ITERATIONS;
            averageBFPortion /= ITERATIONS;
            averageBPPortion /= ITERATIONS;
            System.out.println("Average run-time\t:\t" + averageRt);
            System.out.println("Average portion of BB time\t:\t" + averageBBPortion);
            System.out.println("Average portion of BP time\t:\t" + averageBPPortion);
            System.out.println("Average portion of BF time\t:\t" + averageBFPortion);
        }
    }
}
