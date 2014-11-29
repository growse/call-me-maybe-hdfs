package com.growse.hdfs_partition_test;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

public class HdfsWriterThread extends Thread {
    private static String hdfsPath = "/partitiontester/";
    private final static Log LOG = LogFactory.getLog(HdfsWriterThread.class);
    private int offset;
    private int count;
    private Path testpath = new Path(hdfsPath);
    private Random random;
    private FileSystem fs;
    private Map<Integer, byte[]> confirmedMap;
    private List<Integer> failList;

    public HdfsWriterThread(Integer offset, Integer count, Map<Integer, byte[]> confirmedMap, List<Integer> failList) throws IOException {
        this.offset = offset;
        this.count = count;
        Configuration configuration = new Configuration();
        this.fs = FileSystem.get(configuration);
        this.confirmedMap = confirmedMap;
        this.failList = failList;
        random = new Random(offset + count);
    }

    public void run() {
        byte[] bytes = new byte[1024];
        int value = offset;
        while (value < count) {
            LOG.info(String.format("Trying to write %d", value));
            Path filepath = new Path(testpath, String.valueOf(value));
            FSDataOutputStream stream;
            random.nextBytes(bytes);
            try {
                stream = fs.create(filepath);
                stream.write(bytes);
                stream.close();
                confirmedMap.put(value, bytes.clone());
            } catch (IOException e) {
                e.printStackTrace();
                failList.add(value);
            }
            value += 5;
        }
    }

    public static void Setup() throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Path testdir = new Path(hdfsPath);
        if (fs.exists(testdir)) {
            fs.delete(testdir, true);
        }
        LOG.info("Creating " + hdfsPath);
        fs.mkdirs(testdir);
    }

    public static void EvaluateResults(Integer count, Map<Integer, byte[]> confirmedMap, List<Integer> failList) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] writtenfiles = fs.listStatus(new Path(hdfsPath));
        SortedMap<Integer, byte[]> writtenMap = new TreeMap<>();
        for (FileStatus status : writtenfiles) {
            byte[] actualbytes = IOUtils.toByteArray(new DataInputStream(fs.open(status.getPath())));
            writtenMap.put(Integer.parseInt(status.getPath().getName()), actualbytes);
        }

        ArrayList<Integer> thereAndConfirmed = new ArrayList<>();
        ArrayList<Integer> confirmedButNotThere = new ArrayList<>();
        ArrayList<Integer> thereButNotConfirmed = new ArrayList<>();

        for (int thingie : confirmedMap.keySet()) {
            if (writtenMap.keySet().contains(thingie) && Arrays.equals(writtenMap.get(thingie), confirmedMap.get(thingie))) {
                thereAndConfirmed.add(thingie);
                if (writtenMap.get(thingie).length == 0) {
                    LOG.info(String.format("Empty file: %d", thingie));
                }
            } else {
                confirmedButNotThere.add(thingie);
            }
        }

        for (int thingie : writtenMap.keySet()) {
            LOG.info(String.format("There: %d", thingie));
            if (!confirmedMap.keySet().contains(thingie)) {
                thereButNotConfirmed.add(thingie);
            }
        }
        LOG.info(String.format("%d total", count));
        LOG.info(String.format("%d acknowledged", confirmedMap.size()));
        LOG.info(String.format("%d survivors", thereAndConfirmed.size()));
        LOG.info(String.format("%d acknowledged writes lost", confirmedButNotThere.size()));
        LOG.info(String.format("ACK failed: %s", StringUtils.join(",", failList)));
    }
}
