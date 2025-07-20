package l2;

import java.util.List;
import org.scify.jedai.blockbuilding.ExtendedSuffixArraysBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.ComparisonCleaningMethod;

/**
 *
 * @author G_A.Papadakis
 */
public class ExtendedSuffixArrays {

    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        String mainDir = "/home/gap2/NetBeansProjects/jedai/data/cleanCleanErDatasets/";
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};
        
        int[] bbConf = {10, 36, 414, 37, 199, 429, 104, 103, 419, 469};
        int[] wScheme = {14, 10, 8, 10, 10, 7, 10, 10, 6, 12};
        ComparisonCleaningMethod[] mbAlgorithm = {ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING,
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.WEIGHTED_EDGE_PRUNING, 
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.RECIPROCAL_WEIGHTING_NODE_PRUNING, 
            ComparisonCleaningMethod.RECIPROCAL_WEIGHTING_NODE_PRUNING, 
            ComparisonCleaningMethod.BLAST,
            ComparisonCleaningMethod.BLAST, 
            ComparisonCleaningMethod.RECIPROCAL_CARDINALITY_NODE_PRUNING, 
            ComparisonCleaningMethod.CARDINALITY_NODE_PRUNING
        };

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2[datasetId]);
            List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final ExtendedSuffixArraysBlocking esab = new ExtendedSuffixArraysBlocking();
            esab.setNumberedGridConfiguration(bbConf[datasetId]);
            List<AbstractBlock> blocks = esab.getBlocks(profiles1, profiles2);

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

                blocks = esab.getBlocks(profiles1, profiles2);
                
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
