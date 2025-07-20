package defaultmethods.der;

import java.util.List;
import org.scify.jedai.blockbuilding.QGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.WeightedEdgePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author G_A.Papadakis
 */
public class DefaultQGrams {

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

            final QGramsBlocking qb = new QGramsBlocking(6);
            List<AbstractBlock> blocks = qb.getBlocks(profiles1);

            final BlockFiltering bf = new BlockFiltering(0.50f);
            blocks = bf.refineBlocks(blocks);

            final IBlockProcessing bpMethod = new WeightedEdgePruning(WeightingScheme.ECBS);
            blocks = bpMethod.refineBlocks(blocks);

            final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageRt = 0;
            double averageBBPortion = 0;
            double averageBFPortion = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = qb.getBlocks(profiles1);
                
                long time2 = System.currentTimeMillis();
                 
                blocks = bf.refineBlocks(blocks);
                
                long time3 = System.currentTimeMillis();
                
                blocks = bpMethod.refineBlocks(blocks);
                
                long time4 = System.currentTimeMillis();

                double rt = time4 - time1;
                averageRt += rt;
                averageBBPortion += (time2 - time1) / rt;
                averageBFPortion += (time3 - time2) / rt;
                System.out.println("Run-time\t:\t" + rt);
            }
            averageRt /= ITERATIONS;
            averageBBPortion /= ITERATIONS;
            averageBFPortion /= ITERATIONS;
            System.out.println("Average run-time\t:\t" + averageRt);
            System.out.println("Average portion of BB time\t:\t" + averageBBPortion);
            System.out.println("Average portion of BF time\t:\t" + averageBFPortion);
        }
    }
}
