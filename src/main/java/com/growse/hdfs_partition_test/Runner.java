package com.growse.hdfs_partition_test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Created by andrew on 29/11/2014.
 */
public class Runner {
    public void main(String[] args) throws IllegalAccessException, InvocationTargetException, InterruptedException, IOException, InstantiationException, NoSuchMethodException {
        if (args.length < 2) {
            System.out.println("Usage: partitiontesthdfs <count>");
            System.exit(1);
        }
        int count = Integer.parseInt(args[1]);
        RunPartitionTest(HdfsWriterThread.class,count);
    }

    private static final Log LOG = LogFactory.getLog(Runner.class);

    public static void RunPartitionTest(Class<? extends Thread> writerThreadClass, int count) throws IOException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException, InterruptedException {
        final Map<Integer, byte[]> confirmedMap = Collections.synchronizedSortedMap(new TreeMap<Integer, byte[]>());
        final List<Integer> failList = Collections.synchronizedList(new ArrayList<Integer>());
        writerThreadClass.getDeclaredMethod("Setup").invoke(null);
        Thread t1 = writerThreadClass.getDeclaredConstructor(Integer.class, Integer.class, Map.class, List.class).newInstance(0, count, confirmedMap, failList);
        Thread t2 = writerThreadClass.getDeclaredConstructor(Integer.class, Integer.class, Map.class, List.class).newInstance(1, count, confirmedMap, failList);
        Thread t3 = writerThreadClass.getDeclaredConstructor(Integer.class, Integer.class, Map.class, List.class).newInstance(2, count, confirmedMap, failList);
        Thread t4 = writerThreadClass.getDeclaredConstructor(Integer.class, Integer.class, Map.class, List.class).newInstance(3, count, confirmedMap, failList);
        Thread t5 = writerThreadClass.getDeclaredConstructor(Integer.class, Integer.class, Map.class, List.class).newInstance(4, count, confirmedMap, failList);

        LOG.info("Starting Threads");
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        LOG.info("Waiting for join");
        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();
        LOG.info("Finished");
        LOG.info("Confirmed:");
        System.out.println("Heal cluster");
        System.in.read();

        writerThreadClass.getDeclaredMethod("EvaluateResults", Integer.class, Map.class, List.class).invoke(null, count, confirmedMap, failList);
    }
}
