package blockingWorkflows.schemaBased;

import Utilities.*;
import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;

/**
 *
 * @author Georgios
 */
public class ParameterFreeWorkflow {
    
    private final static int ITERATIONS = 10;

    public static void main(String[] args) {
        BasicConfigurator.configure();

        String mainDir = "data/";
        String[] attributesD1 = {"http://www.okkam.org/ontology_restaurant1.owl#name", "name", "title", "title", "https://www.scads.de/movieBenchmark/ontology/name", 
            "https://www.scads.de/movieBenchmark/ontology/name", "https://www.scads.de/movieBenchmark/ontology/title", "title", "title", "title"};
        String[] attributesD2 = {"http://www.okkam.org/ontology_restaurant2.owl#name", "name", "title", "title", "https://www.scads.de/movieBenchmark/ontology/name", 
            "https://www.scads.de/movieBenchmark/ontology/name", "https://www.scads.de/movieBenchmark/ontology/title", "title", "title", "title"};
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        for (int datasetId = 0; datasetId < groundtruthDirs.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasetsD1[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            profiles1 = ProfileProcessing.reduceProfilesToAttribute(profiles1, attributesD1[datasetId]);
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2[datasetId]);
            List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            profiles2 = ProfileProcessing.reduceProfilesToAttribute(profiles2, attributesD2[datasetId]);
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final StandardBlocking sb = new StandardBlocking();
            List<AbstractBlock> blocks = sb.getBlocks(profiles1, profiles2);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(true);
            List<AbstractBlock> bpBlocks = cbbp.refineBlocks(blocks);
            
            final ComparisonPropagation cp = new ComparisonPropagation();
            blocks = cp.refineBlocks(bpBlocks);

            final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageBp = 0;
            double averageBu = 0;
            double averageMb = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();
                
                blocks = sb.getBlocks(profiles1, profiles2);
                
                long time2 = System.currentTimeMillis();
                
                bpBlocks = cbbp.refineBlocks(blocks);
                
                long time3 = System.currentTimeMillis();
                
                cp.refineBlocks(bpBlocks);
            
                long time4 = System.currentTimeMillis();
                
                averageBp += time3 - time2;
                averageBu += time2 - time1;
                averageMb += time4 - time3;
            }
            averageBu /= ITERATIONS;
            averageBp /= ITERATIONS;
            averageMb /= ITERATIONS;
            System.out.println("Average block building run-time\t:\t" + averageBu);
            System.out.println("Average block purging run-time\t:\t" + averageBp);
            System.out.println("Average meta-blocking run-time\t:\t" + averageMb);
        }
    }
}
