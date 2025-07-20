package l2.schemaBased;

import java.util.List;
import org.scify.jedai.blockbuilding.SuffixArraysBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.ComparisonCleaningMethod;

/**
 *
 * @author G_A.Papadakis
 */
public class SuffixArrays {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/schemaBased/";
        String[] attributes = {"Name", "Name", "Title", "Title", "Title", "Title"};
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "walmartProfiles", "dblpProfiles2"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "amazonProfiles2", "scholarProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "amazonWalmartIdDuplicates", "dblpScholarIdDuplicates"};
        
        int[] bbConf = {49, 170, 340, 480, 474, 499};
        int[] wScheme = {0, 14, 14, 6, 12, 0};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_NODE_PRUNING
        };

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

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

            final SuffixArraysBlocking sab = new SuffixArraysBlocking();
            sab.setNumberedGridConfiguration(bbConf[datasetId]);
            List<AbstractBlock> blocks = sab.getBlocks(profiles1, profiles2);

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

                blocks = sab.getBlocks(profiles1, profiles2);
                
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
