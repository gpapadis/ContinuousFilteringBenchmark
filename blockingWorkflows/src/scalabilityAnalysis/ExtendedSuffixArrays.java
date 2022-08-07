/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scalabilityAnalysis;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.ExtendedSuffixArraysBlocking;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.blockprocessing.IBlockProcessing;
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

/**
 *
 * @author Georgios
 */
public class ExtendedSuffixArrays {

    private final static int ITERATIONS = 10;
    
    public static void main(String[] args) {
        BasicConfigurator.configure();

        String mainDir = "/home/data/syntheticData/";
        String[] datasets = {"10K", "50K", "100K", "200K", "300K", "1M", "2M"};

        for (int datasetId = 0; datasetId < datasets.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + datasets[datasetId]);

            IEntityReader eReader = new EntitySerializationReader(mainDir + datasets[datasetId] + "profiles");
            List<EntityProfile> profiles = eReader.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + datasets[datasetId] + "IdDuplicates");
            final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            int bbcId = 38;
            int mbcId = 5;
            for (int iterations = 0; iterations < ITERATIONS; iterations++) {
                long time1 = System.currentTimeMillis();

                IBlockBuilding blbuMethod = new ExtendedSuffixArraysBlocking();
                blbuMethod.setNumberedGridConfiguration(bbcId);

                final List<AbstractBlock> blocks = blbuMethod.getBlocks(profiles);

                IBlockProcessing bpMethod = new WeightedEdgePruning();
                bpMethod.setNumberedGridConfiguration(mbcId);

                final List<AbstractBlock> mbBlocks = bpMethod.refineBlocks(new ArrayList(blocks));

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
