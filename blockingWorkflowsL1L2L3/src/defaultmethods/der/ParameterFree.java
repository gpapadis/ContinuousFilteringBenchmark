package defaultmethods.der;

import java.util.List;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;

/**
 *
 * @author G_A.Papadakis
 */
public class ParameterFree {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/revision/";
        String[] datasetsD1 = {"cddbProfiles", "coraProfiles", "cddbtitle", "coratitle"};
        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final StandardBlocking sb = new StandardBlocking();
            List<AbstractBlock> blocks = sb.getBlocks(profiles1);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(false);
            List<AbstractBlock> bpBlocks = cbbp.refineBlocks(blocks);

            final ComparisonPropagation cp = new ComparisonPropagation();
            blocks = cp.refineBlocks(bpBlocks);

            final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageRt = 0;
            double averageBBPortion = 0;
            double averageBPPortion = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = sb.getBlocks(profiles1);

                long time2 = System.currentTimeMillis();

                blocks = cbbp.refineBlocks(blocks);

                long time3 = System.currentTimeMillis();

                blocks = cp.refineBlocks(blocks);

                long time4 = System.currentTimeMillis();

                double rt = time4 - time1;
                averageRt += rt;
                averageBBPortion += (time2 - time1) / rt;
                averageBPPortion += (time3 - time2) / rt;
                System.out.println("Run-time\t:\t" + rt);
            }
            averageRt /= ITERATIONS;
            averageBBPortion /= ITERATIONS;
            averageBPPortion /= ITERATIONS;
            System.out.println("Average run-time\t:\t" + averageRt);
            System.out.println("Average portion of BB time\t:\t" + averageBBPortion);
            System.out.println("Average portion of BP time\t:\t" + averageBPPortion);
        }
    }
}
