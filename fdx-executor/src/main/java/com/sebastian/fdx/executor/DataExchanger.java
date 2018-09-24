package com.sebastian.fdx.executor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.sebastian.fdx.fs.api.BaseFileSystem;
import com.sebastian.fdx.fs.api.Conf;
import com.sebastian.fdx.fs.api.FileStatus;

import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;

public class DataExchanger {
    //非空文件
    private final List<FileInfo> allNoneEmptyFiles = new ArrayList<FileInfo>();

    //传输失败的文件
    private List<FileInfo> failedFiles = Collections.synchronizedList(new ArrayList<FileInfo>());

    private List<FileInfo> notDealedFiles = Collections.synchronizedList(new ArrayList<FileInfo>());

    //传输完的字节计数
    private AtomicLong byteCounter = new AtomicLong(0);

    //传输成功的文件计数
    private AtomicInteger successedFileCounter = new AtomicInteger(0);

    //创建成功的空目录计数
    private int successedEmptyDirCounter;

    private long totalByte;
    private int totalFileCount;

    //源目录列表
    private List<String> srcPathList;

    //目标目录
    private String destPath;

    private int threadCount = 1;
    private int bufferSize;

    //源filesystem
    BaseFileSystem srcFs;

    //目标filesystem
    BaseFileSystem destFs;

    private AtomicInteger runningThreadCounter;

    /**
     * @param srcPathList  待迁移的数据目录
     * @param destPath     目标目录
     */
    public DataExchanger(List<String> srcPathList, String destPath) {
        this.srcPathList = srcPathList;
        this.destPath = destPath;
    }

    /**
     * 根据parameter.xml中的配置进行初始化，最终生成源和目标文件系统
     * @param parameterXml parameter.xml的路径
     * @throws Exception
     */
    private void init(String parameterXml) throws Exception {
        try {
            Map<String, String> parameters = Utils.parseXml(parameterXml);
            Utils.log("parameters:" + parameters);

            threadCount = Utils.parseInt(Constans.THREAD_COUNT_KEY, Constans.DEFAULT_THREAD_COUNT);
            bufferSize = Utils.parseInt(Constans.BUFFER_SIZE_KEY, Constans.DEFAULT_BUFFER_SIZE);
            threadCount = (threadCount < 1 ? Constans.DEFAULT_THREAD_COUNT : (threadCount > Constans.MAX_THREAD_COUNT ? Constans.MAX_THREAD_COUNT : threadCount));

            String destFsImpl = parameters.get(Constans.DEST_FILESYSTEM_IMPLEMENT);
            destFs = getFs(parameters.get(Constans.DEST_HDFS_RESOURCE_PATH_KEY), destFsImpl, getConf(parameters, Constans.DEST_PREFIX));
            Utils.log("init destFs[ " + destFs + " ] success!");

            String srcFsImpl = parameters.get(Constans.SRC_FILESYSTEM_IMPLEMENT);
            srcFs = getFs(parameters.get(Constans.SRC_HDFS_RESOURCE_PATH_KEY), srcFsImpl, getConf(parameters, Constans.SRC_PREFIX));
            Utils.log("init srcFs[ " + srcFs + " ] success!");
        } catch (Exception e) {
            throw new Exception("DataExchanger init failed.", e);
        }
    }

    private Conf getConf(Map<String, String> parameters, String prefix) {
        Conf conf = new Conf();
        conf.addProperty(Conf.HDFS_CONF_PATH, parameters.get(prefix + Conf.HDFS_CONF_PATH));
        return conf;
    }

    /**
     * 利用jetty的WebAppClassLoader从给定的资源路径加载不同版本hadoop的FileSystem
     * @param resourcePath  hadoop FileSystem相关的hdfs-site.xml，core-sete.xml, jars等文件
     * @param className  BaseFileSystem实现类
     * @param conf
     * @return
     * @throws Exception
     */
    private BaseFileSystem getFs(String resourcePath, String className, Conf conf) throws Exception {
        ClassLoader old = Thread.currentThread().getContextClassLoader();
        try {
            WebAppContext appContext = new WebAppContext();
            WebAppClassLoader appClassLoader = new WebAppClassLoader(appContext);
            Set<String> resourceFiles = new HashSet<String>();
            Utils.getAllResouceFile(new File(resourcePath), resourceFiles);
            for (String res : resourceFiles) {
                appClassLoader.addClassPath(res);
            }

            Thread.currentThread().setContextClassLoader(appClassLoader);
            Class<?> clazz = appClassLoader.loadClass(className);
            BaseFileSystem fs = (BaseFileSystem) clazz.newInstance();
            fs.initFileSystem(conf);
            return fs;
        } catch (Exception e) {
            throw new Exception("create filesystem from " + resourcePath, e);
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
    }

    /**
     * 获取basePath下所有文件和空目录
     * @param fs
     * @param basePath
     * @return basePath下的所有空目录集合和文件集合
     * @throws Exception
     */
    public Map<String, Set<FileInfo>> getFileInfo(BaseFileSystem fs, String basePath) throws Exception {
        Map<String, Set<FileInfo>> result = new HashMap<String, Set<FileInfo>>();
        if (fs.exists(basePath) == false) {
            Utils.log("srcpath " + basePath + " dose not exist.");
            return result;
        }

        FileStatus base = fs.getFileStatus(basePath);
        Set<FileInfo> emptyDirs = new HashSet<FileInfo>();
        Set<FileInfo> files = new HashSet<FileInfo>();
        recurseFileInfo(fs, base, emptyDirs, files);

        result.put(Constans.DIR, emptyDirs);
        result.put(Constans.FILE, files);
        return result;
    }

    /**
     * 遍历出basePath下所有文件、空目录
     * @param fs        文件系统
     * @param basePath  遍历的目录
     * @param emptyDirs     保存空目录的Set
     * @param files    保存文件的Set
     * @throws Exception
     */
    private void recurseFileInfo(BaseFileSystem fs, FileStatus basePath, Set<FileInfo> emptyDirs, Set<FileInfo> files) throws Exception {
        if (basePath.isIsdir() == false) {
            files.add(new FileInfo(basePath.getPath().toString(), basePath.getLength()));
            return;
        }

        FileStatus[] children = fs.listStatus(basePath.getPath());
        if (children == null || children.length < 1) {
            emptyDirs.add(new FileInfo(basePath.getPath().toString(), -1));
        } else {
            for (FileStatus child : children) {
                recurseFileInfo(fs, child, emptyDirs, files);
            }
        }
    }

    /**
     * 将源文件系统上src目录下的所有子目录分类到空目录集合(emptyDirs)、空文件集合(emptyFiles)、非空文件集合(allNoneEmptyFiles)，
     * 并配置每个子目录在目标文件系统中的路径
     * @param src   源目录
     * @param emptyDirs   空目录集合
     * @param emptyFiles  空文件集合
     * @throws Exception
     */
    private void preCopy(String src, List<FileInfo> emptyDirs, List<FileInfo> emptyFiles) throws Exception {
        //获取源文件、空目录列表
        Map<String, Set<FileInfo>> srcFileInfo = null;
        try {
            srcFileInfo = getFileInfo(srcFs, src);
        } catch (Exception e) {
            throw new Exception(String.format("get source files from %s failed.", src), e);
        }

        if(srcFileInfo.size() < 1) {
            return;
        }

        int dirCount = 0;
        int emptyFileCount = 0;
        Set<FileInfo> dirs = srcFileInfo.get(Constans.DIR);
        Set<FileInfo> files = srcFileInfo.get(Constans.FILE);

        String srcParentPath = srcFs.getParent(src);
        if (files != null && files.size() > 0) {
            totalFileCount += files.size();
            for (FileInfo file : files) {
                file.setOutput(destPath + File.separator + Utils.makeRelative(srcParentPath, file.getInput()));
                long fileLength = file.getFileLength();
                if (fileLength == 0L) {
                    emptyFiles.add(file);
                    emptyFileCount++;
                    continue;
                }
                totalByte += fileLength;
                allNoneEmptyFiles.add(file);
            }
        }

        if (dirs != null && dirs.size() > 0) {
            dirCount = dirs.size();
            for (FileInfo dir : dirs) {
                dir.setOutput(destPath + File.separator + Utils.makeRelative(srcParentPath, dir.getInput()));
                emptyDirs.add(dir);
            }
        }
        Utils.log(String.format("src path %s has %d empty dirs %d files %d empty files and %d none empty files.)", src, dirCount, files.size(), emptyFileCount, (files.size() - emptyFileCount)));
    }

    /**
     * 开始copy文件
     *  step1:目录分类成空目录、空文件、非空文件
     *  step2:创建空目录
     *  step3:创建空文件
     *  step4:启动多线程copy非空文件
     * @throws Exception
     */
    public void run() throws Exception {
        if (destFs.exists(destPath) && destFs.getFileStatus(destPath).isIsdir() == false) {
            throw new Exception(String.format("destition [%s] exists but not a dir", destPath));
        }

        List<FileInfo> emptyDirs = new ArrayList<FileInfo>();
        List<FileInfo> emptyFiles = new ArrayList<FileInfo>();

        for (String src : srcPathList) {
            preCopy(src, emptyDirs, emptyFiles);
        }

        int totalNoneEmptyFileCount = allNoneEmptyFiles.size();

        //空目录直接创建
        for (FileInfo dir : emptyDirs) {
            if (destFs.exists(dir.getOutput()) == false) {
                destFs.mkdirs(dir.getOutput());
                successedEmptyDirCounter++;
            }
        }
        //空文件直接创建
        for (FileInfo file : emptyFiles) {
            if (destFs.exists(file.getOutput()) == false) {
                try {
                    destFs.createNewFile(file.getOutput());
                    successedFileCounter.incrementAndGet();
                } catch (Exception exp) {
                    throw new Exception("create empty file:" + file + " failed.", exp);
                } finally { }
            }
        }

        if (totalNoneEmptyFileCount > 0) {
            threadCount = threadCount > totalNoneEmptyFileCount ? totalNoneEmptyFileCount : threadCount;
            threadCount = Constans.MIN_BYTE < totalByte ? threadCount : 1;
            for (FileInfo file : allNoneEmptyFiles) {
                notDealedFiles.add(file);
            }
            startExecutor(threadCount);
            runningThreadCounter = new AtomicInteger(threadCount);
            new Monitor().start();
        }
    }

    /**
     * 创建copy 文件的线程
     * @param executorCount  线程数量
     * @throws Exception
     */
    private void startExecutor(int executorCount) throws Exception {
        Utils.log("thread count:" + executorCount);
        int id = 0;
        while (id < executorCount) {
            Executor exec = new Executor(id);
            exec.start();
            id++;
        }
    }

    /**
     * 执行数据读取、写入的线程
     */
    class Executor extends Thread {
        private int id;

        Executor(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            FileInfo fileInfo = null;
            while ((fileInfo = getNotDealedFile()) != null) {
                String destPath = fileInfo.getOutput();
                try {
                    if (destFs.exists(destPath)) {
                        Utils.log(destPath + " already exists");
                    } else {
                        copy(srcFs, destFs, fileInfo);
                    }
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            runningThreadCounter.decrementAndGet();
        }
    }

    private FileInfo getNotDealedFile() {
        FileInfo file = null;
        if (notDealedFiles.size() > 0) {
            file = notDealedFiles.remove(0);
        }
        return file;
    }

    /**
     * 从源文件系统srcFs读取文件到本地内存， 然后再写写入目标文件系统
     * @param srcFs
     * @param destFs
     * @param fileInfo
     * @throws Exception
     */
    public void copy(BaseFileSystem srcFs, BaseFileSystem destFs, FileInfo fileInfo) throws Exception {
        DataInputStream dis = null;
        DataOutputStream dos = null;
        String srcFile = fileInfo.getInput();
        String destFile = fileInfo.getOutput();
        try {
            dis = srcFs.open(srcFile);
            dos = destFs.create(destFile);
            byte[] buffer = new byte[bufferSize];
            int size = 0;
            long byteCount = 0;

            while ((size = dis.read(buffer)) > 0) {
                dos.write(buffer, 0, size);
                byteCount += size;
            }
            dos.flush();

            byteCounter.addAndGet(byteCount);
            successedFileCounter.addAndGet(1);
        } catch (Exception exp) {
            try {
                destFs.delete(destFile, true);
            } catch (Exception ex) {
                Utils.log("clean " + destFile + " failed.", ex);
            }
            throw new Exception("copy " + fileInfo + " failed.", exp);
        } finally {
            Utils.close(dis);
            Utils.close(dos);
        }
    }

    /**
     * 监控线程
     * 获取当前进度
     */
    class Monitor extends Thread {

        private void printProgress(){
            Utils.log("***bytePercent=>" + (byteCounter.get() + "/" + totalByte)
                    + "***fileCountPercent=>" + (successedFileCounter.get() + "/" + totalFileCount
                    + "***emptyDirCountPercent=>" + successedEmptyDirCounter + "/" + successedEmptyDirCounter));

            if (successedFileCounter.get() + failedFiles.size() >= totalFileCount || runningThreadCounter.get() <= 0) {
                Utils.log("srcPathList:" + srcPathList
                        + Constans.LINE_SEPARATOR + "targetPath:" + destPath
                        + Constans.LINE_SEPARATOR + "failed file count::" + failedFiles.size());

                if (failedFiles.size() > 0) {
                    for (FileInfo file : failedFiles) {
                        Utils.log("failed:" + file.toString());
                    }
                    System.exit(2);
                }
                System.exit(0);
            }

            try {
                if (totalByte - byteCounter.get() < 2 * 1024 * 1024L) {
                    Thread.sleep(1 * 1000L);
                } else {
                    Thread.sleep(15 * 1000L);
                }
            } catch (InterruptedException e) {
                System.exit(-1);
            }
        }

        @Override
        public void run() {
            while (true) {
                printProgress();
            }
        }
    }

    /**
     * @param:文件路径 源路径1 源路径2...  目标路径
     */
    public static void main(String[] args) {
        if (args == null || args.length < 3) {
            System.err.println("Usage:/app/parameter.xml /src1 /src2 /dest");
            System.exit(-1);
        }

        List<String> srcPath = new ArrayList<String>(args.length - 2);
        for (int i = 1; i < args.length - 1; ++i) {
            srcPath.add(args[i]);
        }

        DataExchanger executor = new DataExchanger(srcPath, args[args.length - 1]);
        try {
            executor.init(args[0]);
            executor.run();
        } catch (Exception e) {
            Utils.log("execute failed.", e);
            System.exit(-1);
        }
    }
}
