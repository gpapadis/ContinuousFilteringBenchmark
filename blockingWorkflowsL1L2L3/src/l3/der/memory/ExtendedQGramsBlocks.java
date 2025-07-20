package l3.der.memory;

import java.util.List;
import org.scify.jedai.blockbuilding.ExtendedQGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
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
public class ExtendedQGramsBlocks {

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/revision/";
        String[] datasets = {"cddbProfiles", "coraProfiles", "cddbtitle", "coratitle"};
        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};

        int[] q = {8, 11, 0, 0};
        int[] blockPurging = {-1, -1, 1, -1};
        int[] bfRatio = {4, 30, 38, 39};
        int[] wScheme = {6, 11, 1, 1};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.CARDINALITY_EDGE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.CARDINALITY_NODE_PRUNING
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
    
            final ExtendedQGramsBlocking qb = new ExtendedQGramsBlocking();
            qb.setNumberedGridConfiguration(q[datasetId]);
            List<AbstractBlock> blocks = qb.getBlocks(profiles);

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

            IBlockProcessing bpMethod = null;
            final ComparisonPropagation cp = new ComparisonPropagation();
            if (mbAlgorithm[datasetId] != null) {
                bpMethod = ComparisonCleaningMethod.getDefaultConfiguration(mbAlgorithm[datasetId]);
                bpMethod.setNumberedGridConfiguration(wScheme[datasetId]);
                blocks = bpMethod.refineBlocks(blocks);
            } else {
                blocks = cp.refineBlocks(blocks);
            }
            
//            System.gc();
            long memory3 = runtime.totalMemory() - runtime.freeMemory();
            double blocksMemory = (memory3 - memory2) / (1024.0 * 1024.0);
            System.out.println("Blocks memory\t:\t" + blocksMemory);
            double totalMemory = (memory3 - memory1) / (1024.0 * 1024.0);
            System.out.println("Total memory\t:\t" + totalMemory);
        }
    }
}
