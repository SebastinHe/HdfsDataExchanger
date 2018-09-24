package com.sebastian.fdx.executor;

public class Constans {

    static final String BUFFER_SIZE_KEY = "buffer.size";
    static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    static final String THREAD_COUNT_KEY = "thread.count";

    static final String RETRY_COUNT_KEY = "retry.count";
    static final int DEFAULT_RETRY_COUNT = 3;

    static final int DEFAULT_THREAD_COUNT = 2;
    static final int MAX_THREAD_COUNT = 20;

    //总字节数多余MIN_BYTE才启动多个线程
    static final long MIN_BYTE = 1024 * 1024 * 10;

    static final String SPLITER = "\001";


    static final String DEST_PREFIX = "dest.";
    static final String SRC_PREFIX = "src.";

    static final String DEST_HDFS_RESOURCE_PATH_KEY = DEST_PREFIX + "hdfs.resource.path";
    static final String SRC_HDFS_RESOURCE_PATH_KEY = SRC_PREFIX + "hdfs.resource.path";

    static final String DEST_FILESYSTEM_IMPLEMENT = DEST_PREFIX + "filesystem.implement";
    static final String SRC_FILESYSTEM_IMPLEMENT = SRC_PREFIX + "filesystem.implement";

    static final String LINE_SEPARATOR = System.getProperty("line.separator");

    static final String DIR = "dirs";
    static final String FILE = "files";


}
