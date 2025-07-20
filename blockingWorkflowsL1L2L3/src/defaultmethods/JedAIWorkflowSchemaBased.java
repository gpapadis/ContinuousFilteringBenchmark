package defaultmethods;

import java.util.List;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityNodePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author G_A.Papadakis
 */
public class JedAIWorkflowSchemaBased {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/schemaBased/";
        String[] attributes = {"Name", "Name", "Title", "Title", "Title", "Title"};
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "walmartProfiles", "dblpProfiles2"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "amazonProfiles2", "scholarProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "amazonWalmartIdDuplicates", "dblpScholarIdDuplicates"};

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            String sourcePath = mainDir + datasetsD1[datasetId] + "_" + attributes[datasetId];
            EntitySerializationReader reader = new EntitySerializationReader(sourcePath);
            List<EntityProfile> profiles1 = reader.getEntityProfiles();
            System.out.println("Source Entities: " + profiles1.size());

            // read target entities
            String targetPath = mainDir + datasetsD2[datasetId] + "_" + attributes[datasetId];
            reader = new EntitySerializationReader(targetPath);
            List<EntityProfile> profiles2 = reader.getEntityProfiles();
            System.out.println("Target Entities: " + profiles2.size());

            // read ground-truth file
            String groundTruthPath = mainDir + groundtruthDirs[datasetId];
            GtSerializationReader gtReader = new GtSerializationReader(groundTruthPath);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());
            
            final IBlockBuilding blockBuildingMethod = new StandardBlocking();
            List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profiles1, profiles2);

            final IBlockProcessing blockCleaningMethod1 = new ComparisonsBasedBlockPurging(true);
            blocks = blockCleaningMethod1.refineBlocks(blocks);

            final IBlockProcessing blockCleaningMethod2 = new BlockFiltering();
            blocks = blockCleaningMethod2.refineBlocks(blocks);

            final IBlockProcessing comparisonCleaningMethod = new CardinalityNodePruning(WeightingScheme.JS);
            blocks = comparisonCleaningMethod.refineBlocks(blocks);

            final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageRt = 0;
            double averageBBPortion = 0;
            double averageBFPortion = 0;
            double averageBPPortion = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = blockBuildingMethod.getBlocks(profiles1, profiles2);

                long time2 = System.currentTimeMillis();

                blocks = blockCleaningMethod1.refineBlocks(blocks);

                long time3 = System.currentTimeMillis();

                blocks = blockCleaningMethod2.refineBlocks(blocks);

                long time4 = System.currentTimeMillis();

                blocks = comparisonCleaningMethod.refineBlocks(blocks);

                long time5 = System.currentTimeMillis();

                double rt = time5 - time1;
                averageRt += rt;
                averageBBPortion += (time2 - time1) / rt;
                averageBPPortion += (time3 - time2) / rt;
                averageBFPortion += (time4 - time3) / rt;
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
