package minhash;

import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;

import java.util.List;
import java.util.Set;

public class Reader {

    public static List<EntityProfile> readSerialized(String path){
        EntitySerializationReader reader = new EntitySerializationReader(path);
        return reader.getEntityProfiles();
    }

    public static Set<IdDuplicates> readSerializedGT(String path, List<EntityProfile> sourceEntities, List<EntityProfile> targetEntities){
        GtSerializationReader gtReader = new GtSerializationReader(path);
        return gtReader.getDuplicatePairs(sourceEntities, targetEntities);
    }
}
