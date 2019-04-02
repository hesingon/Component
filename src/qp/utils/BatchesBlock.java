/**
 * Contains a vector for all batches and a vector for all tuples in batches
 */
package qp.utils;

import java.util.Vector;
import java.io.Serializable;

public class BatchesBlock implements Serializable {
    int MAX_SIZE;
    int pageSize;
    Vector batchesVector;
    Vector tuplesVector;

    public BatchesBlock(int numPage, int pageSize) {
        MAX_SIZE = numPage;
        this.pageSize = pageSize;
        batchesVector = new Vector(MAX_SIZE);
        tuplesVector = new Vector(MAX_SIZE * pageSize);
    }

    public Vector<Batch> getBatchesVector() {
        return batchesVector;
    }

    public int addBatch(Batch batch) {
        if(!isFull()) {
            batchesVector.add(batch);
            for (int i = 0; i < batch.size(); i++) {
                tuplesVector.add(batch.elementAt(i));
            }
            return 1;
        }
        return 0;
    }

    public void addTuples(Vector tupleList) {
        Batch batch = new Batch(pageSize);
        for(int i = 0; i < tupleList.size(); i++) {
            if(batch.isFull()) {
                batchesVector.add(batch);
                batch = new Batch(pageSize);
            }
            batch.add((Tuple) tupleList.get(i));
            tuplesVector.add((Tuple) tupleList.get(i));
        }
        if(!batch.isEmpty()) {
            batchesVector.add(batch);
        }
    }

    public Vector getTuples() {
        return tuplesVector;
    }

    public int getTupleSize() {
        return tuplesVector.size();
    }

    public boolean isEmpty() {
        return batchesVector.isEmpty();
    }

    public boolean isFull() {
        return getTupleSize() >= MAX_SIZE * pageSize;
    }
}