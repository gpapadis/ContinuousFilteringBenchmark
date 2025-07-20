package defaultmethods.der.memory;

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
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;

/**
 *
 * @author G_A.Papadakis
 */
public class ParameterFree {

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/revision/";
        String[] datasetsD1 = {"cddbProfiles", "coraProfiles", "cddbtitle", "coratitle"};
        String[] groundtruthDirs = {"cddbIdDuplicates", "coraIdDuplicates", "cddbIdDuplicates", "coraIdDuplicates"};

        Runtime runtime = Runtime.getRuntime();
        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

            System.gc();
            long memory1 = runtime.totalMemory() - runtime.freeMemory();

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            System.gc();
            long memory2 = runtime.totalMemory() - runtime.freeMemory();

            final StandardBlocking sb = new StandardBlocking();
            List<AbstractBlock> blocks = sb.getBlocks(profiles1);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(false);
            List<AbstractBlock> bpBlocks = cbbp.refineBlocks(blocks);

            final ComparisonPropagation cp = new ComparisonPropagation();
            blocks = cp.refineBlocks(bpBlocks);
            
//            System.gc();
            long memory3 = runtime.totalMemory() - runtime.freeMemory();
            double blocksMemory = (memory3 - memory2) / (1024.0 * 1024.0);
            System.out.println("Blocks memory\t:\t" + blocksMemory);
            double totalMemory = (memory3 - memory1) / (1024.0 * 1024.0);
            System.out.println("Total memory\t:\t" + totalMemory);
        }
    }
}
