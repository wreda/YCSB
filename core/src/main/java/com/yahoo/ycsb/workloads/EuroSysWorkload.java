package com.yahoo.ycsb.workloads;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.FileGenerator;

/**
 * Workload generator for BRB's EuroSys submission attempt
 * <p>
 * Properties to control the client:
 * </p>
 * <UL>
 * <LI><b>disksize</b>: how many bytes of storage can the disk store? (default 100,000,000)
 * <LI><b>occupancy</b>: what fraction of the available storage should be used? (default 0.9)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian or latest (default: histogram)
 * </ul> 
 *
 *
 * @author waleed
 *
 */
public class EuroSysWorkload extends CoreWorkload {   

    /**
     * The type of workload that is being utilized.
     */
    public static final String WORKLOADTYPE_PROPERTY="worktype";

    /**
     * The default type of workload to utilize.
     */
    public static final String WORKLOADTYPE_PROPERTY_DEFAULT="synthetic";

    public static String worktype;
    
    /**
     * The path of file to use for trace-type workloads.
     */
    public static final String TRACEFILE_PROPERTY="tracefile";

    /**
     * The default path of the trace file.
     */
    public static final String TRACEFILE_PROPERTY_DEFAULT="inputFile.txt";

    public static String tracefile;
    
    FileGenerator filegen;
    
    @Override
    public void init(Properties p) throws WorkloadException
    {
        worktype = p.getProperty(WORKLOADTYPE_PROPERTY, WORKLOADTYPE_PROPERTY_DEFAULT);
        
        //check if trace or synthetic workload
        //if trace initialize bufferreader (thread safe?) and calculate min max keys then persist
        if(worktype.compareTo("trace") == 0)
        {
            tracefile = p.getProperty(TRACEFILE_PROPERTY, TRACEFILE_PROPERTY_DEFAULT);
            filegen = new FileGenerator(tracefile);
            File statsFile = new File(tracefile+"stats");
            //Check if stats file exists for trace
            if(statsFile.exists() && statsFile.canRead())
            {
                //Read min/max key values and task counts for insertions
                try
                {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tracefile+"stats"));
                    HashMap<String, Integer> statsMap = (HashMap<String, Integer>)ois.readObject();
                    System.out.printf("Finished reading persisted trace stats. Output: " + statsMap);
                    setTraceProperties(statsMap, p);
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                //Calculate trace stats
                List<String> task = new ArrayList<String>();
                int taskcount = 0;
                int maxkey = 0;
                int minkey = Integer.MAX_VALUE;
                while(true)
                {
                    task = readTransactionFromFile(filegen);
                    if(task == null)
                        break;
                    for(String req: task)
                    {
                        int reqInt = Integer.parseInt(req);
                        if(reqInt > maxkey)
                            maxkey = reqInt;
                        if(reqInt < minkey)
                            minkey = reqInt;
                    }
                    taskcount++;
                }
                
                //Reload file
                filegen.reloadFile();
                
                //Add stats to map and persist
                HashMap<String, Integer> statsMap = new HashMap<String, Integer>();
                statsMap.put("minkey", minkey);
                statsMap.put("maxkey", maxkey);
                statsMap.put("taskcount", taskcount);
                System.out.printf("Calculated trace statistics. Output: " + statsMap);               
                setTraceProperties(statsMap, p);
                
                try
                {
                   FileOutputStream fos = new FileOutputStream(tracefile+"stats");
                   ObjectOutputStream oos = new ObjectOutputStream(fos);
                   oos.writeObject(statsMap);
                   oos.close();
                   fos.close();
                   System.out.printf("Trace statistics have been persisted in: " + tracefile + "stats");
                }
                catch(IOException ioe)
                 {
                       ioe.printStackTrace();
                 }
                
            }
        }

        super.init(p);
    }

    @Override
    public void doTransactionRead(DB db)
    {
        if(worktype.compareTo("trace") == 0)
        {
            try
            {
                List<String> task = readTransactionFromFile(filegen);
                //System.out.println("Reading the following keys: "+task);
                for (int i = 0; i < task.size(); i++) {
                    long keyInt = Utils.hash(Integer.parseInt(task.get(i)));
                    keyInt = keyInt%100000;
                    
                    //int keyInt = Integer.parseInt(task.get(i));
                    String kname = buildKeyName(keyInt);
                    task.set(i, kname);
                }
                
                HashSet<String> fields=null;

                if (!readallfields)
                {
                    //read a random field
                    String fieldname=fieldnames.get(Integer.parseInt(fieldchooser.nextString()));

                    fields=new HashSet<String>();
                    fields.add(fieldname);
                }

                db.readMulti(table,new HashSet<String>(task),fields,new Vector<HashMap<String,ByteIterator>>());
    
            }
            catch (UnsupportedOperationException e)
            {
                e.printStackTrace();
            }
        }
        else
            super.doTransactionRead(db);
    }
    
    /**
     * Reads and parses next line in the workload trace
     * @throws UnsupportedOperationException 
     */
    private List<String> readTransactionFromFile(FileGenerator filegen) throws UnsupportedOperationException
    {
        String line = filegen.nextString();
        if(line == null)
            return null;
        List<String> task;
        line.replaceAll("\n", "");
        if(line.startsWith("R "))
            task = new ArrayList(Arrays.asList(line.split(" ")));
        else
            throw new UnsupportedOperationException();
        task.remove(0); //remove R
        task.remove(task.size()-1); //remove interarrival modifier (no longer used)
        return task;
    }
    
    /**
     * Sets relevant properties using gathered trace stats
     * @param statsMap
     * @param p
     */
    private void setTraceProperties(HashMap<String, Integer> statsMap, Properties p)
    {
        Integer keyDiff = statsMap.get("maxkey") - statsMap.get("minkey");
        
        p.setProperty(INSERT_START_PROPERTY,statsMap.get("minkey").toString());
        p.setProperty(Client.RECORD_COUNT_PROPERTY, keyDiff.toString());
        p.setProperty(Client.OPERATION_COUNT_PROPERTY,statsMap.get("taskcount").toString());
    }
}

