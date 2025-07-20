package l2.der.memory;

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
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.ComparisonCleaningMethod;

/**
 *
 * @author G_A.Papadakis
 */
public class TokenBlocking {

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/revision/";
        String[] datasets = {"cddbProfiles", "coraProfiles", "cddbtitle", "coratitle"};
        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};
 
        int[] blockPurging = {1, 1, 1, -1};
        int[] bfRatio = {4, 26, 33, -1};
        int[] wScheme = {0, 6, 0, 6};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.BLAST
        };

        Runtime runtime = Runtime.getRuntime();
        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasets[datasetId]);

            System.gc();
            long memory1 = runtime.totalMemory() - runtime.freeMemory();
            
            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasets[datasetId]);
            List<EntityProfile> profiles = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            System.gc();
            long memory2 = runtime.totalMemory() - runtime.freeMemory();
            
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

//            System.gc();
            long memory3 = runtime.totalMemory() - runtime.freeMemory();
            double blocksMemory = (memory3 - memory2) / (1024.0 * 1024.0);
            System.out.println("Blocks memory\t:\t" + blocksMemory);
            double totalMemory = (memory3 - memory1) / (1024.0 * 1024.0);
            System.out.println("Total memory\t:\t" + totalMemory);
        }
    }
}
