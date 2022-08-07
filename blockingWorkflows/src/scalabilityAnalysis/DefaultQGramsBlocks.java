/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scalabilityAnalysis;

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
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author Georgios
 */
public class DefaultQGramsBlocks {

    private final static int ITERATIONS = 10;
    
    public static void main(String[] args) {
        BasicConfigurator.configure();

        String mainDir = "/home/data/syntheticData/";
        String[] datasets = {"10K", "50K", "100K", "200K", "300K", "1M", "2M"};

        for (int datasetId = 0; datasetId < datasets.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasets[datasetId]);

            IEntityReader eReader = new EntitySerializationReader(mainDir + datasets[datasetId] + "profiles");
            final List<EntityProfile> profiles = eReader.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + datasets[datasetId] + "IdDuplicates");
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());
            for (int iterations = 0; iterations < ITERATIONS; iterations++) {
                long time1 = System.currentTimeMillis();

                final QGramsBlocking qb = new QGramsBlocking(6);
                final List<AbstractBlock> blocks = qb.getBlocks(profiles);

                final BlockFiltering bf = new BlockFiltering(0.50f);
                final List<AbstractBlock> bfBlocks = bf.refineBlocks(blocks);

                final IBlockProcessing bpMethod = new WeightedEdgePruning(WeightingScheme.ECBS);
                List<AbstractBlock> mbBlocks = bpMethod.refineBlocks(bfBlocks);

                long time2 = System.currentTimeMillis();
                System.out.println("Run-time\t:\t" + (time2 - time1));
                if (iterations == 0) {
                    BlocksPerformance blStats = new BlocksPerformance(mbBlocks, duplicatePropagation);
                    blStats.setStatistics();
                    blStats.printStatistics(0, "", "");
                }
            }
        }
    }
}
