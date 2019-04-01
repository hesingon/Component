/**
 * To eliminate all duplicates of the projections
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;

import java.util.HashMap;
import java.util.Vector;

public class Distinct extends Operator {

    Operator base;
    Vector attrSet;
    int batchsize, bufferNum;  // number of tuples per outbatch
    HashMap<Integer, Vector> hm;


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

        if (base.open())
            return true;
        else
            return false;
    }

    /** Read next tuple from operator */

    public Batch next() {
        System.out.println("Project:-----------------in next-----------------");
        outbatch = new Batch(batchsize);
        inbatch = base.next();
        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            int partitionNum = _hashTuple(inbatch.elementAt(i), bufferNum);
//            System.out.print(partitionNum);

        }

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

    private int _hashTuple(Tuple tuple, int partitions) {
        int hashInput = 0;
        for (Object col_value : tuple.data()) {
            if (col_value instanceof Integer) {
                hashInput += (int) col_value;
            } else {
                hashInput += _stringHash(col_value);
            }
        }
        return hashInput % partitions;
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

    private void _generatePartitions() {

    }

}
