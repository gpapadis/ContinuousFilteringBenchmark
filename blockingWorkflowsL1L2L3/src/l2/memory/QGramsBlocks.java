package l2.memory;

import java.util.List;
import org.scify.jedai.blockbuilding.QGramsBlocking;
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
public class QGramsBlocks {

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/cleanCleanErDatasets/";
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        int[] q = {2, 2, 4, 4, 4, 4, 4, 3, 1, 2};
        int[] blockPurging = {-1, -1, 1, 1, -1, -1, -1, 1, -1, 1};
        int[] bfRatio = {1, 3, 37, 3, 16, 34, 8, 11, 20, 37};
        int[] wScheme = {0, 10, 12, 5, 7, 3, 11, 11, 6, 6};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_WEIGHTING_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING,
            ComparisonCleaningMethod.BLAST,
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
            
            final QGramsBlocking qb = new QGramsBlocking();
            qb.setNumberedGridConfiguration(q[datasetId]);
            List<AbstractBlock> blocks = qb.getBlocks(profiles1, profiles2);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(true);
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
            long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
            
            double mbs = (memoryAfter - memoryBefore)/(1024.0*1024.0);
            System.out.println("Consumed memory (mbs)\t:\t" + mbs);
            double totalMbs = (memoryAfter - totalMemory)/(1024.0*1024.0);
            System.out.println("Total memory (mbs)\t:\t" + totalMbs);
        }
    }
}
