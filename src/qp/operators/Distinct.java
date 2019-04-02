/**
 * To eliminate all duplicates of the projections
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;

import java.util.HashMap;
import java.util.Vector;

// todo: this Distinct operator does not include the PROJECT function
public class Distinct extends Operator {
    private static final int VARIANT_1 = 1;
    private static final int VARIANT_2 = 2;

    Operator base;
    Vector attrSet;
    int batchsize, bufferNum;  // number of tuples per outbatch
    HashMap<Integer, Batch> partitionHash, dedupHash;
    private Vector<Batch> PagesOnDisk = new Vector<Batch>();
    private Vector<Batch> distinctOutput = new Vector<Batch>(); // all distinct tuples'll be packed into the batches
    int batchNum = 0; //this is the index of the next batch to be returned from distinctOutput

    Batch inbatch;
    Batch outbatch;


    public Distinct(Operator base) {
        super(OpType.DISTINCT);
        this.base = base;

        // Schema after DISTINCT operation is the same. Attribute list, tuple-size (in bytes) are the same.
        this.setSchema(base.getSchema());
    }

    /** Opens the connection to the base operator
     ** Also figures out what are the columns to be
     ** projected from the base operator
     **/

    public boolean open() {
        /** setnumber of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        bufferNum = BufferManager.getNumBuffer();
        partitionHash = new HashMap<Integer, Batch>();

        _populateHash(partitionHash);

        if (!base.open())
            return false;
        else {
            // Partitioning phase
            while ((inbatch = base.next()) != null && inbatch.size() != 0) {
                outbatch = new Batch(batchsize);
                if (inbatch == null) {
                    break;
                }
                Tuple currTuple;
                Batch currBatch;
                for (int i = 0; i < inbatch.size(); i++) {
                    // todo: set the limit for each Batch by using isFull()
                    currTuple = inbatch.elementAt(i);
                    int partitionNum = _hashTuple(currTuple, bufferNum, VARIANT_1);
                    currBatch = partitionHash.get(partitionNum);
                    try {
                        currBatch.add(currTuple);
                    } catch (NullPointerException e) {
                        System.out.println(e);
                    }


                    if (currBatch.isFull()) {
                        PagesOnDisk.add(partitionHash.remove(i));
                        partitionHash.put(i, new Batch(batchsize));
                    }
                }
            }

            // partitioning done, now write all pages to disk
            for (int i = 0; i < bufferNum - 1; i++) {
                PagesOnDisk.add(partitionHash.remove(i));
            }

            //Duplicate Elimination phase
            dedupHash = new HashMap<Integer, Batch>();
            _populateHash(dedupHash);

            Tuple currTuple;
            Batch currBacket;
            for (Batch curr : PagesOnDisk) {
                for (int i = 0; i < curr.size(); i++) {
                    currTuple = curr.elementAt(i);
                    int partitionNum = _hashTuple(currTuple, bufferNum, VARIANT_2);
                    currBacket = dedupHash.get(partitionNum);

                    if (currBacket.size() == 0)
                        currBacket.add(currTuple);
                    else {
                        boolean isDuplicate = false;
                        for (int j = 0; j < currBacket.size(); j++) {
                            if (Tuple.isDuplicate(currTuple, currBacket.elementAt(j))) {
                                isDuplicate = true;
                                break;
                            }
                        }
                        if (!isDuplicate)
                            currBacket.add(currTuple);
                    }

                }
            }

            // pact all tuples from the hash table into batches.
            _packTuplesFromHashtableToBatches();

            return true;
        }

    }

    /** Read next tuple from operator */

    public Batch next() {
        System.out.println("Project:-----------------in next-----------------");
        try {
            inbatch = distinctOutput.get(batchNum++);
        } catch (ArrayIndexOutOfBoundsException e) {
            inbatch = null;
        }

        //            System.out.print(partitionNum);
        return inbatch;
    }


    /** Close the operator */
    public boolean close() {
        return true;
        /*
	if(base.close())
	    return true;
	else
	    return false;
	    **/
    }


    // hashVariant is used to vary the hash function
    private int _hashTuple(Tuple tuple, int partitions, int hashVariant) {
        int hashInput = 0;
        for (Object col_value : tuple.data()) {
            if (col_value instanceof Integer) {
                hashInput += (int) col_value;
            } else {
                hashInput += _stringHash(col_value);
            }
        }
        return (hashVariant * hashInput + (hashInput % hashVariant)) % (partitions - 1);
    }

    private int _stringHash(Object input) {
        char letters[];
        letters = ((String) input).toCharArray();
        int sum = 0;
        for (char letter : letters) {
            sum += letter;
        }
        return sum;
    }


    private void _populateHash(HashMap<Integer, Batch> hashtable) {
        for (int i = 0; i < bufferNum - 1; i++) {
            hashtable.put(i, new Batch(batchsize));
        }
    }

    private void _packTuplesFromHashtableToBatches() {
        Batch storebatch = new Batch(batchsize);
        Batch hashbatch;
        for (int i = 0; i < this.dedupHash.size(); i++) {
            hashbatch = this.dedupHash.get(i);

            while (hashbatch.size() != 0) {

                storebatch.add(hashbatch.removeAndGetTail());

                if (storebatch.isFull()) {
                    this.distinctOutput.add(storebatch);
                    storebatch = new Batch(batchsize);
                }
            }
        }
        if (storebatch.size() != 0)
            this.distinctOutput.add(storebatch);
    }

}
