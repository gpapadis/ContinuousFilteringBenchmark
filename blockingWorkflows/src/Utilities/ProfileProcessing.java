package Utilities;

import java.util.ArrayList;
import java.util.List;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;

/**
 *
 * @author gap2
 */
public class ProfileProcessing {
    
    public static List<EntityProfile> reduceProfilesToAttribute(List<EntityProfile> profiles, String attribute) {
        final List<EntityProfile> reducedProfiles = new ArrayList<>();
        for (EntityProfile profile : profiles) {
            EntityProfile reducedProfile = new EntityProfile(profile.getEntityUrl());
        
            for (Attribute a : profile.getAttributes()) {
                if (a.getName().equals(attribute)) {
                    reducedProfile.addAttribute(a.getName(), a.getValue());
                }
            }
            
            reducedProfiles.add(reducedProfile);
        }
        return reducedProfiles;
    }
}
