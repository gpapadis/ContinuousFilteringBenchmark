package l1;

import java.util.List;
import org.scify.jedai.blockbuilding.QGramsBlocking;
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
 * @author G_A.Papadakis
 */
public class QGramsBlocks {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/cleanCleanErDatasets/";
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        int[] q = {4, 2, 4, 4, 4, 4, 4, 4, 1, 6};
        int[] blockPurging = {-1, 1, 1, 1, 1, -1, -1, 1, -1, -1};
        int[] bfRatio = {0, 2, 27, 2, 16, 26, 6, 10, 20, 23};
        int[] wScheme = {7, 7, 12, 6, 10, 0, 0, 11, 6, 3};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_WEIGHTING_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.BLAST
        };

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

            final QGramsBlocking qb = new QGramsBlocking();
            qb.setNumberedGridConfiguration(q[datasetId]);
            List<AbstractBlock> blocks = qb.getBlocks(profiles1, profiles2);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(true);
            if (0 < blockPurging[datasetId]) {
                blocks = cbbp.refineBlocks(blocks);
                System.out.println("BP in");
            } else {
                System.out.println("BP out");
            }

            final BlockFiltering bf = new BlockFiltering();
            if (0 <= bfRatio[datasetId]) {
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

                blocks = qb.getBlocks(profiles1, profiles2);

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
