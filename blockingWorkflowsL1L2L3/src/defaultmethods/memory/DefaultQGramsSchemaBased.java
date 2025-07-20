package defaultmethods.memory;

import java.util.List;
import org.scify.jedai.blockbuilding.QGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.WeightedEdgePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author G_A.Papadakis
 */
public class DefaultQGramsSchemaBased {

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/schemaBased/";
        String[] attributes = {"Name", "Name", "Title", "Title", "Title", "Title"};
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "walmartProfiles", "dblpProfiles2"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "amazonProfiles2", "scholarProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "amazonWalmartIdDuplicates", "dblpScholarIdDuplicates"};

        Runtime runtime = Runtime.getRuntime();
        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);
            
            System.gc();
            long totalMemory = runtime.totalMemory() - runtime.freeMemory();
            
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

            System.gc();
            long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
            
            final QGramsBlocking qb = new QGramsBlocking(6);
            List<AbstractBlock> blocks = qb.getBlocks(profiles1, profiles2);

            final BlockFiltering bf = new BlockFiltering(0.50f);
            blocks = bf.refineBlocks(blocks);

            final IBlockProcessing bpMethod = new WeightedEdgePruning(WeightingScheme.ECBS);
            blocks = bpMethod.refineBlocks(blocks);

//            System.gc();
            long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
            
            double mbs = (memoryAfter - memoryBefore)/(1024.0*1024.0);
            System.out.println("Consumed memory (mbs)\t:\t" + mbs);
            double totalMbs = (memoryAfter - totalMemory)/(1024.0*1024.0);
            System.out.println("Total memory (mbs)\t:\t" + totalMbs);
        }
    }
}
