package blockingWorkflows.schemaBased;

import Utilities.*;
import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.QGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.WeightedEdgePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author Georgios
 */
public class DefaultQGramsBlocks {

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

            final QGramsBlocking qb = new QGramsBlocking(6);
            System.out.println(qb.getMethodConfiguration());

            List<AbstractBlock> blocks = qb.getBlocks(profiles1, profiles2);
            
            final BlockFiltering bf = new BlockFiltering(0.50f);
            System.out.println(bf.getMethodConfiguration());

            List<AbstractBlock> bfBlocks = bf.refineBlocks(blocks);

            final IBlockProcessing bpMethod = new WeightedEdgePruning(WeightingScheme.ECBS);
            System.out.println("MB=" + bpMethod.getMethodName());
            System.out.println(bpMethod.getMethodConfiguration());

            List<AbstractBlock> mbBlocks = bpMethod.refineBlocks(bfBlocks);

            final BlocksPerformance blStats = new BlocksPerformance(mbBlocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(0, "", "");

            double averageBf = 0;
            double averageBu = 0;
            double averageMb = 0;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long time1 = System.currentTimeMillis();

                blocks = qb.getBlocks(profiles1, profiles2);

                long time2 = System.currentTimeMillis();

                bfBlocks = bf.refineBlocks(blocks);

                long time3 = System.currentTimeMillis();

                bpMethod.refineBlocks(bfBlocks);

                long time4 = System.currentTimeMillis();

                averageBf += time3 - time2;
                averageBu += time2 - time1;
                averageMb += time4 - time3;
            }
            averageBf /= ITERATIONS;
            averageBu /= ITERATIONS;
            averageMb /= ITERATIONS;
            System.out.println("Average block building run-time\t:\t" + averageBu);
            System.out.println("Average block filtering run-time\t:\t" + averageBf);
            System.out.println("Average meta-blocking run-time\t:\t" + averageMb);
        }
    }
}
