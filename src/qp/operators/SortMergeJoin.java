/**
 * Sort Merge Join Algorithm
 */
package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.RandNumb;
import qp.utils.Tuple;
import java.util.Vector;

public class SortMergeJoin extends Join {
    int batchsize;      // Number of tuples per out batch

    int leftindex;      // Index of the join attribute in left table
    int rightindex;
    int leftBatchSize;
    int rightBatchSize;

    int leftCur;    // Cursor for left side buffer
    int rightCur;    // Cursor for right side buffer
    int tempcurs;

    Attribute leftattr;
    Attribute rightattr;
    Batch outbatch;     // Output batch
    Batch leftBatch;    // Buffer for left input stream
    Batch rightBatch;   // Buffer for right input stream

    /*
     * Sorted table stored in disk, next() function get the next batch of sorted tuples
     */
    ExternalMergeSort sortedLeft;
    ExternalMergeSort sortedRight;

    Tuple leftTuple;
    Tuple rightTuple;
    Tuple refTuple;
    Vector tempBlock;

    boolean reachEnd;  // either of the tables reaches the end

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/

    @Override
    public boolean open() {
        System.out.println("SortMergeJoin: -----------------in open--------------");
        Debug.PPrint(schema);

        /** select number of tuples per batch and number of batches per block **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        leftattr = con.getLhs();
        rightattr = (Attribute) con.getRhs();

        Vector<Attribute> leftSet = new Vector<>();
        Vector<Attribute> rightSet = new Vector<>();
        leftSet.add(leftattr);
        rightSet.add(rightattr);

        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        leftBatchSize = Batch.getPageSize() / left.getSchema().getTupleSize();
        rightBatchSize = Batch.getPageSize() / right.getSchema().getTupleSize();

        sortedLeft = new ExternalMergeSort(left, leftSet, optype, numBuff, "LeftSort" + RandNumb.randInt(100000, 999999));
        sortedRight = new ExternalMergeSort(right, rightSet, optype, numBuff, "RightSort" + RandNumb.randInt(100000,999999));

        if (!left.open() || !right.open()) {
            return false;
        }

        /** Both right table and left side table
         ** are sorted here at the beginning
         **/
        if (!sortedLeft.open() || !sortedRight.open()) {
            System.out.println("SortMergeJoin: Could not open left or right table");
            return false;
        }

        // Initialize for the while loop
        tempBlock = new Vector();
        tempcurs = 0;
        reachEnd = false;
        leftCur = 0; rightCur = 0;

        // Get first batch of sorted tuples
        leftBatch = sortedLeft.next();
        rightBatch = sortedRight.next();

        refTuple = rightBatch.elementAt(0);
        saveRightTableSameTuples();

        return true;
    }



    /** from input buffers selects the tuples satisfying join condition
     * And returns a page of output tuples
     */
    public Batch next() {
        System.out.println("SortMergeJoin:-----------------in next--------------" + "debug: 09");

        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {

            /**
             * Handle the case right/left batch reaches the end
             * process tuples in tempBlock according to equality
             */
            if (reachEnd) {
                if(tempBlock != null && !tempBlock.isEmpty()) {
                    while (!outbatch.isFull()) {
                        leftTuple = leftBatch.elementAt(leftCur);
                        int diff = Tuple.compareTuples(leftTuple, refTuple, leftindex, rightindex);

                        if (diff < 0) {
                            if (!processNextLeftCur()) return outbatch;

                        } else if (diff == 0) {
                            while (tempcurs < tempBlock.size()) {
                                outbatch.add(leftTuple.joinWith((Tuple) tempBlock.get(tempcurs)));
                                tempcurs++;
                                // return when outbatch is full, leftover will be handled at the start of the next run
                                if (outbatch.isFull()) {
                                    return outbatch;
                                }
                            }
                            if (tempcurs == tempBlock.size()) {
                                tempcurs = 0;
                                if (!processNextLeftCur()) return outbatch;
                            } else {
                                System.out.println("Error: SortMergeJoin: tempBlock not fully traversed!");
                            }

                        } else if (diff > 0) {
                            tempBlock.clear();
                            tempcurs = 0;
                            return outbatch;
                        }
                    }
                } else {
                    // Nothing to process further
                    close();
                    return null;
                }
            }

            /**
             * No table reached the end, able to proceed
             */
            leftTuple = leftBatch.elementAt(leftCur);
            int diff = Tuple.compareTuples(leftTuple, refTuple, leftindex, rightindex);

            // left tuple and right tuple have the same attribute value
            if (diff == 0) {
                // handle the equal condition
                if (!handleMatch()) return outbatch;

                // Verify join completion
                if (tempcurs == tempBlock.size()) {
                    tempcurs = 0;
                    if (!processNextLeftCur()) return outbatch;
                } else {
                    System.out.println("Error: SortMergeJoin: tempBlock not fully traversed!");
                }

            } else if (diff < 0) {
                // leftTuple smaller than right, leftTuple move to the next and continue checking
                if (!processNextLeftCur()) return outbatch;

            } else if (diff > 0) {
                // leftTuple larger than right, move to next right tuple if possible
                tempBlock.clear();
                tempcurs = 0;

                // Move to next
                while (Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex) > 0) {
                    rightCur++;
                    if (rightCur == rightBatch.size()) {
                        rightCur = 0;
                        rightBatch = sortedRight.next();
                        if(rightBatch == null) {
                            reachEnd = true;
                            break;
                        }
                    }
                    rightTuple = rightBatch.elementAt(rightCur);
                }

                // Check whether rightTable reaches the end
                if(rightBatch == null) {
                    reachEnd = true;
                    break;
                }

                refTuple = rightBatch.elementAt(rightCur);

                saveRightTableSameTuples();
            }
        }
        return outbatch;
    }

    /**
     * join leftTuple with tuples in tempBlock
     * @return true: outBatch OK to go      false: outBatch is full
     */
    public boolean handleMatch() {
        while (tempcurs < tempBlock.size()) {
            outbatch.add(leftTuple.joinWith((Tuple) tempBlock.get(tempcurs)));
            tempcurs++;

            /**
             * If the outBatch is full, then return the batch
             * Other matching tuples can be handled in next iteration
             */
            if (outbatch.isFull()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Call this function to handle next value of leftCur
     * @return true: hasNext    false: doesn't have next
     */
    private boolean processNextLeftCur() {
        leftCur++;
        if (leftCur == leftBatch.size()) { // Move to next batch is necessary
            leftBatch = sortedLeft.next();
            leftCur = 0;
            if (leftBatch == null) { // Should not continue for this round
                reachEnd = true;
                tempBlock.clear();
                return false;
            }
        }
        return true;
    }

    /**
     * Move current right tuple to the tempBlock if they have same value
     * @return a boolean for successful execution
     */
    private boolean saveRightTableSameTuples() {
        while (rightBatch != null) {
            rightTuple = rightBatch.elementAt(rightCur);

            if (Tuple.compareTuples(refTuple, rightTuple, rightindex) == 0) {
                tempBlock.add(rightTuple);
                rightCur++;

                // In case there are a lot tuples with same merge attribute
                if (rightCur == rightBatch.size()) {
                    rightCur = 0;
                    rightBatch = sortedRight.next();
                    if (rightBatch == null) {
                        reachEnd = true;
                        break;
                    }
                }
            } else {
                break;
            }
        }
        return true;
    }

    public boolean close() {
        sortedLeft.close();
        sortedRight.close();
        return true;
    }
}