package l2.memory;

import java.util.List;
import org.scify.jedai.blockbuilding.ExtendedQGramsBlocking;
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
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.ComparisonCleaningMethod;

/**
 *
 * @author G_A.Papadakis
 */
public class ExtendedQGramsBlocks {

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/cleanCleanErDatasets/";
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        int[] q = {6, 8, 14, 10, 2, 12, 8, 14, 14, 5};
        int[] blockPurging = {-1, -1, 1, 1, 1, -1, 1, 1, -1, -1};
        int[] bfRatio = {0, 31, 36, 2, 18, 38, 17, 13, 12, 29};
        int[] wScheme = {3, 13, 8, 5, 14, 0, 10, 7, 0, 6};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.BLAST
        };

        Runtime runtime = Runtime.getRuntime();
        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

            System.gc();
            long totalMemory = runtime.totalMemory() - runtime.freeMemory();
            
            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2[datasetId]);
            List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            System.gc();
            long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
            
            final ExtendedQGramsBlocking eqb = new ExtendedQGramsBlocking();
            eqb.setNumberedGridConfiguration(q[datasetId]);
            List<AbstractBlock> blocks = eqb.getBlocks(profiles1, profiles2);

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

//            System.gc();
            long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
            
            double mbs = (memoryAfter - memoryBefore)/(1024.0*1024.0);
            System.out.println("Consumed memory (mbs)\t:\t" + mbs);
            double totalMbs = (memoryAfter - totalMemory)/(1024.0*1024.0);
            System.out.println("Total memory (mbs)\t:\t" + totalMbs);
        }
    }
}
