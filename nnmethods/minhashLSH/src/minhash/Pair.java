package minhash;

/**
 *
 * @author Georgios
 */
public class Pair {
   
    private final int entityId1;
    private final int entityId2;
    
    public Pair(int e1, int e2) {
        entityId1 = e1;
        entityId2 = e2;
    }
    
    public int getEntityId1() {
        return entityId1;
    }

    public int getEntityId2() {
        return entityId2;
    }
}
