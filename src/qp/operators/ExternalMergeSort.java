package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;

public class ExternalMergeSort extends Operator {
    protected Operator base;

    int numBuff;
    int batchSize;
    int[] attrIndex;

    Vector attrSet;
    List<File> sortedFiles;
    String fileName;

    ObjectInputStream in;

    public ExternalMergeSort(Operator base, Vector as, int opType, int numBuff, String fileName) {
        super(opType);
        this.base = base;
        this.attrSet = as;
        this.numBuff = numBuff;
        this.fileName = fileName;
    }

    /**
     * Do sorting and save sorted result to disk
     * @return boolean for successful execution
     */
    public boolean open() {

        if (numBuff < 3) { // Check buffer sufficiency
            System.out.println("insufficient buffer for sort merge");
            System.exit(1);
        }

        if(!base.open()) return false;

        int tupleSize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        /** The following loop finds out the index of the columns that
         * are required from the base operator
         */
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;
        }

        sortedFiles = new ArrayList<>();

        /**
         * Key parts: create sorted runs and merge sorted runs
         */
        createSortedRuns();
        mergeSortedFiles();

        try {
            in = new ObjectInputStream(new FileInputStream(sortedFiles.get(0)));
        } catch (Exception e) {
            System.err.println(" Error reading " + sortedFiles.get(0));
            return false;
        }

        return true;
    }

    public Batch next() {
        if(sortedFiles.size() != 1) {
            System.out.println("ExternalMergeSort: sorted file not merged properly. ");
        }

        try {
            Batch thisBatch = (Batch) in.readObject();
            return thisBatch;
        } catch (IOException e) {
            return null;
        } catch (ClassNotFoundException e) {
            System.out.println("File not found. ");
        }
        return null;
    }

    public boolean close() {
        for(File file: sortedFiles)
            file.delete();
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * Generate sorted runs
     * Save the sorted runs to file system
     */
    public void createSortedRuns() {
        Batch batch = base.next();
        int counter = 0;
        while(batch != null && !batch.isEmpty()) {

            // Move the N-2 batches from disk to memory as BatchBlock
            BatchesBlock thisRun = new BatchesBlock(numBuff, batchSize);
            while(!thisRun.isFull() && batch != null && !batch.isEmpty()) {
                thisRun.addBatch(batch);
                batch = base.next();
            }
            counter++;

            // Take out the tuples from BatchBlock and sort
            List<Tuple> allTuples = thisRun.getTuples();
            Collections.sort(allTuples, new AttributeComparator(attrIndex));

            // Save the sorted run to file system and record the File
            BatchesBlock sortedRun = new BatchesBlock(numBuff, batchSize);
            sortedRun.addTuples((Vector) allTuples);
            File result = writeToSortTempFile(sortedRun, counter);
            sortedFiles.add(result);
        }
    }


    /**
     * Do merging until there is only one file
     */
    public void mergeSortedFiles() {

        int counter = 0;
        int runsCounter = 0;
        int inputNumBuff = numBuff - 1;
        List<File> outputSortedFile;

        while (sortedFiles.size() > 1) { // Merge until there is only one file left
            outputSortedFile = new ArrayList<>();
            runsCounter = 0;
            for (int i = 0; i * inputNumBuff < sortedFiles.size(); i++) {

                // Select inputNumBuff sorted runs and do merge
                List<File> currentFilesToBeSort;
                if((i+1) * inputNumBuff >= sortedFiles.size()) {
                    currentFilesToBeSort = sortedFiles.subList(i * inputNumBuff, sortedFiles.size());
                } else {
                    currentFilesToBeSort = sortedFiles.subList(i * inputNumBuff, (i+1) * inputNumBuff);
                }
                File resultFile = mergeSortedRuns(currentFilesToBeSort, counter, runsCounter);

                runsCounter++;
                outputSortedFile.add(resultFile);
            }

            for (File file : sortedFiles) {
                file.delete();
            }
            sortedFiles = outputSortedFile;

            counter++;
        }
    }

    /**
     * Merge multiple files into one file
     * @param runFiles the files to merge
     * @param mergeTimes int for filename
     * @param mergeNumRuns int for filename
     * @return the merged result file
     */
    public File mergeSortedRuns(List<File> runFiles, int mergeTimes, int mergeNumRuns) {

        int inputNumBuff = numBuff - 1;
        int numRuns = runFiles.size();
        int runIndex;
        ArrayList<ObjectInputStream> inputStreams = new ArrayList<>();
        ArrayList<Batch> inputBatchesArray = new ArrayList<>(numRuns);

        // Get all inputStream and get one batch from each inputStream for runFiles
        for (int i = 0; i < numRuns; i++) {
            try {
                ObjectInputStream iStream = new ObjectInputStream(new FileInputStream(runFiles.get(i)));
                inputStreams.add(iStream);
            } catch (IOException e) {
                System.out.println("ExternalMergeSort: Reading temporary file error");
            }

            Batch thisBatch = getNextBatch(inputStreams.get(i));
            inputBatchesArray.add(i, thisBatch);
            if (thisBatch == null) {
                System.out.println("ExternalMergeSort: run " + i + " is empty.");
            }
        }

        File resultFile = new File(this.fileName + "-MergedRunFile-" + mergeTimes + "-" + mergeNumRuns);
        ObjectOutputStream oStream = tryInitObjectOutputStream(resultFile);
        Batch tempBatch;
        Batch outputBuffer = new Batch(batchSize);

        /**
         * Do merging:
         * Use a PriorityQueue to get smallest tuple
         * Use a map to store the runIndex for each tuple
         */
        Queue<Tuple> firstTuplesPQ = new PriorityQueue<>(numRuns, new AttributeComparator(attrIndex));
        Map<Tuple, Integer> runIndexMap = new HashMap<>(numRuns);


        for (runIndex = 0; runIndex < numRuns; runIndex++) {
            tempBatch = inputBatchesArray.get(runIndex);
            Tuple tuple = tempBatch.elementAt(0);
            tempBatch.remove(0);
            firstTuplesPQ.add(tuple);
            runIndexMap.put(tuple, runIndex);
            if (tempBatch.isEmpty()) {
                inputBatchesArray.set(runIndex, getNextBatch(inputStreams.get(runIndex)));
            }
        }

        // Keep adding new tuples and getting top tuples from the firstTuplesPQ
        while (!firstTuplesPQ.isEmpty()) {
            Tuple topTuple = firstTuplesPQ.remove();
            outputBuffer.add(topTuple);

            if (outputBuffer.isFull()) {
                tryWriteToObjectOutputStream(oStream, outputBuffer);
                outputBuffer.clear();
            }

            runIndex = runIndexMap.get(topTuple);
            tempBatch = inputBatchesArray.get(runIndex);

            if (tempBatch != null) {
                Tuple tuple = tempBatch.elementAt(0);
                tempBatch.remove(0);
                firstTuplesPQ.add(tuple);
                runIndexMap.put(tuple, runIndex);
                if (tempBatch.isEmpty()) {
                    inputBatchesArray.set(runIndex, getNextBatch(inputStreams.get(runIndex)));
                }
            }
        }

        // process remaining tuples in outputBuffer
        if (!outputBuffer.isEmpty()) {
            tryWriteToObjectOutputStream(oStream, outputBuffer);
            outputBuffer.clear();
        }
        runIndexMap.clear();

        tryCloseObjectOutputStream(oStream);
        return resultFile;
    }

    /**
     * Get the next Batch from inputStream
     * @return The Batch
     */
    protected Batch getNextBatch(ObjectInputStream inputStream) {
        try {
            Batch batch = (Batch) inputStream.readObject();
            if(batch == null) {
                System.out.println("batch is null");
            }
            return batch;
        } catch (EOFException e) {
            return null;
        } catch (ClassNotFoundException | IOException e) {
            System.out.println("Encounter error when read from stream");
        }
        return null;
    }

    /**
     * Write this run to a temp file
     * @param run The BatchesBlock to be stored in file system
     * @param numRuns The number for file nameing
     * @return The created file, nullable
     */
    public File writeToSortTempFile(BatchesBlock run, int numRuns) {
        try {
            File outputFile = new File(this.fileName + "-ExternalSortTemp-" + numRuns);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(outputFile));

            for (Batch batch : run.getBatchesVector()) {
                out.writeObject(batch);
            }
            out.close();
            return outputFile;
        } catch (IOException io) {
            System.out.println("ExternalMergeSort: Write to file error");
        }
        return null;
    }

    public ObjectOutputStream tryInitObjectOutputStream(File file) {
        try {
            return new ObjectOutputStream(new FileOutputStream(file, true));
        } catch (IOException io) {
            System.out.println("ExternalMergeSort: cannot initialize object output stream");
        }
        return null;
    }

    public void tryWriteToObjectOutputStream(ObjectOutputStream out, Batch batch) {
        try {
            out.writeObject(batch);
            out.reset();          //reset the ObjectOutputStream to enable appending result
        } catch (IOException io) {
            System.out.println("ExternalMergeSort: encounter error when append to object output stream");
        }
    }

    public void tryCloseObjectOutputStream(ObjectOutputStream out) {
        try {
            out.close();
        } catch (IOException io) {
            System.out.println("ExternalMergeSort: could not close output stream");
        }
    }

    class AttributeComparator implements Comparator<Tuple> {
        private int[] attrIndex;

        public AttributeComparator(int[] attrIndex) {
            this.attrIndex = attrIndex;
        }

        @Override
        public int compare(Tuple tuple1, Tuple tuple2) {
            return Tuple.compareTuples(tuple1, tuple2, attrIndex[0]);
        }
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }
}
