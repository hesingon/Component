
import java.util.*;
import java.io.*;

import qp.utils.*;

public class RandomDB {

    private static Random random;
    static boolean[] pk;
    static boolean[] fk;

    public RandomDB() {
        random = new Random(System.currentTimeMillis());
    }


    /**
     * Generates a random string of length equal to range
     **/

    public String randString(int range) {

        String s = "";
        for (int j = 0; j < range; j++)
            s += (new Character((char) (97 + random.nextInt(26)))).toString();
        return s;
    }


    public static void main(String[] args) {

        RandomDB rdb = new RandomDB();

        if (args.length != 2) {
            System.out.println("Usage: java RandomDB <dbname> <numrecords> ");
            System.exit(1);
        }
        String tblname = args[0];
        String srcfile = args[0] + ".det";
        String metafile = args[0] + ".md";
        String datafile = args[0] + ".txt";
        String statfile = args[0] + ".stat";
        int numtuple = Integer.parseInt(args[1]);

        try {
            BufferedReader in = new BufferedReader(new FileReader(srcfile));
            ObjectOutputStream outmd = new ObjectOutputStream(new FileOutputStream(metafile));
            PrintWriter outtbl = new PrintWriter(new BufferedWriter(new FileWriter(datafile)));
            PrintWriter outstat = new PrintWriter(new BufferedWriter(new FileWriter(statfile)));

            outstat.print(numtuple);
            outstat.println();
            /** first line is <number of columns> **/

            String line = in.readLine();
            int numCol = Integer.parseInt(line);
            String[] datatype = new String[numCol];
            int[] range = new int[numCol];
            String[] keytype = new String[numCol];

            /** second line is <size of tuple = number of bytes> **/
            line = in.readLine();
            int size = Integer.parseInt(line);
            //outstat.print(size);
            //outstat.println();


            /** Capture information about data types, range and primary/foreign keys**/
            /** format is <colname><coltype><keytype><attrsize><range>  **/

            /** for schema generation **/
            Vector attrlist = new Vector();
            Attribute attr;
            boolean flag = false;
            int i = 0;


            while ((line = in.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                int tokenCount = tokenizer.countTokens();
                /** get column name **/
                String colname = tokenizer.nextToken();

                /** get data type **/
                datatype[i] = tokenizer.nextToken();

                int type;
                if (datatype[i].equals("INTEGER")) {
                    type = Attribute.INT;
                    //  System.out.println("integer");
                } else if (datatype[i].equals("STRING")) {
                    type = Attribute.STRING;
                    // System.out.println("String");
                } else if (datatype[i].equals("REAL")) {
                    type = Attribute.REAL;
                } else {
                    type = -1;
                    System.err.println("invalid data type");
                    System.exit(1);
                }

                /** range of the values allowed **/
                range[i] = Integer.parseInt(tokenizer.nextToken());


                /** key type PK/FK/NK **/
                keytype[i] = tokenizer.nextToken();
                int typeofkey;
                if (keytype[i].equals("PK")) {
                    pk = new boolean[range[i]];
                    typeofkey = Attribute.PK;

                } else if (keytype[i].equals("FK")) {
                    fk = new boolean[range[i]];
                    typeofkey = Attribute.FK;
                } else {
                    typeofkey = -1;
                }
                int numbytes = Integer.parseInt(tokenizer.nextToken());


                if (typeofkey != -1) {
                    attr = new Attribute(tblname, colname, type);//,typeofkey,numbytes);
                } else {
                    attr = new Attribute(tblname, colname, type, typeofkey);
                }
                attr.setAttrSize(numbytes);
                attrlist.add(attr);
                i++;
            }
            Schema schema = new Schema(attrlist);
            schema.setTupleSize(size);
            outmd.writeObject(schema);
            outmd.close();

            /**
             * Build a hashmap for each col to find different values number
             */
            ArrayList<HashMap<Object, Boolean>> mapList = new ArrayList<>();
            int[] distinceCounts = new int[numCol + 5];
            for (int j = 1; j < numCol; j++) {
                mapList.add(new HashMap<>());
                distinceCounts[j] = 0;
            }

            for (i = 0; i < numtuple; i++) {
//System.out.println("in table generation: "+i);
                int numb = random.nextInt(range[0]);
                while (pk[numb] == true) {
                    numb = random.nextInt(range[0]);
                }
                pk[numb] = true;
                outtbl.print(numb + "\t");

                for (int j = 1; j < numCol; j++) {
                    HashMap<Object, Boolean> thisMap = mapList.get(j-1);
                    if (datatype[j].equals("STRING")) {
                        String temp = rdb.randString(range[j]);
                        outtbl.print(temp + "\t");
                        if (!thisMap.containsKey(temp)){
                            distinceCounts[j]++;
                            thisMap.put(temp, true);
                        }
                    } else if (datatype[j].equals("FLOAT")) {
                        float value = range[j] * random.nextFloat();
                        outtbl.print(value + "\t");
                        if (!thisMap.containsKey(value)){
                            distinceCounts[j]++;
                            thisMap.put(value, true);
                        }
                    } else if (datatype[j].equals("INTEGER")) {
                        int value = random.nextInt(range[j]);
                        outtbl.print(value + "\t");
                        if (keytype[j].equals("FK")) {
                            fk[value] = true;
                        }
                        if (!thisMap.containsKey(value)){
                            distinceCounts[j]++;
                            thisMap.put(value, true);
                        }
                    }
                }
                if (i != numtuple - 1)
                    outtbl.println();

            }
            outtbl.close();

//System.out.println("end of table generation");
            /** printing the number of distinct values of each column
             in <tablename>.stat file
             **/
            /** BUG REPORT
             * always return numtuple for string type
             */
//            for (i = 0; i < numCol; i++) {
//                if (datatype[i].equals("STRING")) {
//                    outstat.print(numtuple + "\t");
//                } else if (datatype[i].equals("FLOAT")) {
//                    outstat.print(numtuple + "\t");
//                } else if (datatype[i].equals("INTEGER")) {
//                    if (keytype[i].equals("PK")) {
//                        int numdist = rdb.getnumdistinct(pk);
//                        outstat.print(numdist + "\t");
//                    } else if (keytype[i].equals("FK")) {
//                        int numdist = rdb.getnumdistinct(fk);
//                        outstat.print(numdist + "\t");
//                    } else {
//                        if (numtuple < range[i])
//                            outstat.print(numtuple + "\t");
//                        else
//                            outstat.print(range[i] + "\t");
//                    }
//
//                }
//            }

            for (i = 0; i < numCol; i++) {
                if (datatype[i].equals("INTEGER")) {
                    if (keytype[i].equals("PK")) {
                        int numdist = rdb.getnumdistinct(pk);
                        outstat.print(numdist + "\t");
                    } else if (keytype[i].equals("FK")) {
                        int numdist = rdb.getnumdistinct(fk);
                        outstat.print(numdist + "\t");
                    } else {
                        outstat.print(distinceCounts[i] + "\t");
                    }

                } else {
                    outstat.print(distinceCounts[i] + "\t");
                }
            }

            outstat.close();
            in.close();
        } catch (IOException io) {
            System.out.println("error in IO ");
            System.out.println(io);
            System.exit(1);
        }

    }

    public int getnumdistinct(boolean[] key) {
        int lenght = key.length;
        int count = 0;
        for (int i = 0; i < lenght; i++) {
            if (key[i] == true) count++;
        }
        return count;
    }
}
