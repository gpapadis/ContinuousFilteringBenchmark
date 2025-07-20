package l3.der;

import java.util.List;
import org.scify.jedai.blockbuilding.SuffixArraysBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
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
public class SuffixArrays {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        String mainDir = "/home/servers/conf/data/";
        String[] datasets = {"cddbProfiles", "coraProfiles", "cddbtitle", "coratitle"};
        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};
//        String[] datasets = {"cddbProfiles", "coraProfiles", "10Kaddress_1", "cddbtitle", "coratitle"};
//        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "10KIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};

        int[] bbConf = {59, 485, 425, 490};
        int[] wScheme = {6, 6, 5, 1};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING
        };

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasets[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasets[datasetId]);
            List<EntityProfile> profiles = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final SuffixArraysBlocking sa = new SuffixArraysBlocking();
            sa.setNumberedGridConfiguration(bbConf[datasetId]);
            List<AbstractBlock> blocks = sa.getBlocks(profiles);

            final IBlockProcessing bpMethod = ComparisonCleaningMethod.getDefaultConfiguration(mbAlgorithm[datasetId]);
            bpMethod.setNumberedGridConfiguration(wScheme[datasetId]);
            blocks = bpMethod.refineBlocks(blocks);

            final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageRt = 0;
            double averagePortion = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = sa.getBlocks(profiles);
                
                long time2 = System.currentTimeMillis();
                
                blocks = bpMethod.refineBlocks(blocks);

                long time3 = System.currentTimeMillis();

                double rt = time3 - time1;
                double bbTime = time2 - time1;
                averagePortion += bbTime / rt;
                averageRt += rt;
                System.out.println("Run-time\t:\t" + rt);
            }
            averageRt /= ITERATIONS;
            averagePortion /= ITERATIONS;
            System.out.println("Average run-time\t:\t" + averageRt);
            System.out.println("Average portion of BB time\t:\t" + averagePortion);
        }
    }
}
