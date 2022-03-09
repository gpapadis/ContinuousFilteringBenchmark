package joins;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.scify.jedai.datamodel.EntityProfile;
import utilities.RepresentationModel;
import utilities.Tokenizer;

/**
 *
 * @author gap2
 */
public class AbstractJoin {
    
    protected final static int ITERATIONS = 10;
    protected static int[] SOURCE_FREQUENCY;

    protected static Map<String, TIntList> indexSource(Tokenizer t, List<EntityProfile> sourceEntities) {
        int id = 0;
        final Map<String, TIntList> index = new HashMap<>();
        for (EntityProfile e : sourceEntities) {
            final Set<String> tokens = RepresentationModel.tokenizeEntity(RepresentationModel.getAttributeValue(e), t);
            for (String token : tokens) {
                TIntList ids = index.get(token);
                if (ids == null) {
                    ids = new TIntArrayList();
                    index.put(token, ids);
                }
                ids.add(id);
            }
            SOURCE_FREQUENCY[id++] = tokens.size();
        }
        return index;
    }
}
