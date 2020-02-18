package cn.trustfar.aiops.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HadoopUtils {
    private static ReadProps readProps = new ReadProps("commo_config.properties");
    private static FileSystem fileSystem = null;
    private static DistributedFileSystem dfs = null;//多namenode方式
    private static Configuration conf = null;
    private static Logger logger = LoggerFactory.getLogger(HadoopUtils.class.getSimpleName());



    /**
     * 删除文件或者目录
     *
     * @param path 文件或者目录路径
     * @param flag true 删除目录，false删除文件
     * @return true成功，false失败
     * @throws Exception
     */
    public static boolean removeFile(String path, boolean flag) throws Exception {
        boolean removeDir = false;
        if (null == dfs) {
            connHadoopByHA();
        }
        removeDir = dfs.delete(new Path(path), flag);
        return removeDir;
    }

    /**
     * /**
     * 根据规则遍历某路径下文件
     *
     * @param path    hdfs路径
     * @param pattern 匹配规则
     * @param isName  true 返回hdfs下的文件名称，false返回hdfs路径下文件的具体路径
     * @return
     * @throws Exception
     */
    public static List<String> getFilePaths(String path, String pattern, boolean isName) throws Exception {
        if (null == dfs) {
            connHadoopByHA();
        }
        List<String> fileNames = new ArrayList<String>();
        Path folderPath = new Path(path);
        FileStatus[] fileStatus = dfs.listStatus(folderPath);
        for (int i = 0; i < fileStatus.length; i++) {
            FileStatus fileStatu = fileStatus[i];
            if (!fileStatu.isDir()) {//仅仅要文件
                Path oneFilePath = fileStatu.getPath();
                if (pattern == null) {
                    fileNames.add(oneFilePath.getName());
                } else {
                    if (oneFilePath.getName().endsWith(pattern)) {
                        String trueFileName = oneFilePath.getName().replaceAll(pattern, "");
                        if (!path.endsWith("/")) {
                            path += "/";
                        }
                        if (isName) {
                            fileNames.add(trueFileName);
                        } else {
                            fileNames.add(path + trueFileName);
                        }
                    }
                }
            }
        }
        return fileNames;
    }

    /**
     * 判断文件或目录是否存在
     *
     * @param filepath 文件路径
     * @return
     */
    public static boolean isExit(String filepath) throws Exception {
        boolean isExit = false;
        if (null == dfs) {
            connHadoopByHA();
        }
        if (dfs.exists(new Path(filepath))) {
            isExit = true;
        }
        return isExit;
    }


    /**
     * 下载文件到本地
     *
     * @param srcPath hdfs路径
     * @param dstPath 本地文件路径
     */
    public static void downloadFile(String srcPath, String dstPath) throws IOException {
        FSDataInputStream in = null;
        FileOutputStream out = null;
        try {
            if (null == dfs) {
                connHadoopByHA();
            }
            in = dfs.open(new Path(srcPath));
            out = new FileOutputStream(dstPath);
            IOUtils.copyBytes(in, out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            out.close();
            in.close();
        }
    }


    /**
     * 判断是否是目录
     *
     * @param srcPath
     * @return
     */
    public static boolean isDirctory(String srcPath) throws Exception {
        if (null == dfs) {
            connHadoopByHA();
        }
        FileStatus fileStatus = null;
        try {
            fileStatus = dfs.getFileStatus(new Path(srcPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (fileStatus.isDirectory()) {
            return true;
        }
        return false;
    }



    /**
     * 通过HA方式连接hdp3.1.0
     *
     * @throws Exception
     */
    public static void connHadoopByHA() throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "trustfar"); //针对hadoop，实际生效
        conf = new Configuration();
//        String ticketCachePath = conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
//        UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCachePath, "trustfar");
        String[] namenodes = {readProps.getProps("hdfs.namenode1"), readProps.getProps("hdfs.namenode2")};//{"nn1", "nn2"};
        String[] namenodesAddr = {readProps.getProps("hdfs.namenodesAddr1"), readProps.getProps("hdfs.namenodesAddr2")};//{"172.16.100.203", "172.16.100.204"};
        String nameservices = readProps.getProps("hdfs.nameservices");//ns1
        conf.set("fs.defaultFS", "hdfs://" + nameservices);
        conf.set("dfs.nameservices", nameservices);//" String nameservices = ns1";
        conf.set("dfs.ha.namenodes." + nameservices, namenodes[0] + "," + namenodes[1]);
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0] + ":8020");
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1] + ":8020");
        conf.set("dfs.client.failover.proxy.provider." + nameservices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        String hdfsRPCUrl = "hdfs://" + nameservices + ":8020";     //HDFS_RPC_PORT=8020 也是从环境变量获取 或 dfs.namenode.rpc-address.cluster1.nn1
        dfs = (DistributedFileSystem) DistributedFileSystem.get(URI.create(hdfsRPCUrl), conf);
        logger.info("******连接hdfs[{}]成功******",hdfsRPCUrl);
    }


    /**
     * 只删除文件夹下面的文件
     * @param hdfsPath hdfs的文件夹路径
     */
    public static void deleteFiles(String hdfsPath) throws Exception {
        if (isExit(hdfsPath)) {
            List<String> filePaths = getFilePaths(hdfsPath, "", false);
            for (String filePath : filePaths) {
                removeFile(filePath, false);
            }
        }
    }

    /**
     *
     *
     *
     */

    /**
     * 通过指定用户名HA方式连接hdp3.1.0
     *
     * @throws Exception
     */
    public static void connHadoopByHA(String userName) throws Exception {
        conf = new Configuration();
        //String ticketCachePath = conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
        // UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCachePath, "trustfar");
        String[] namenodes = {readProps.getProps("hdfs.namenode1"), readProps.getProps("hdfs.namenode2")};//{"nn1", "nn2"};
        String[] namenodesAddr = {readProps.getProps("hdfs.namenodesAddr1"), readProps.getProps("hdfs.namenodesAddr2")};//{"172.16.100.203", "172.16.100.204"};
        String nameservices = readProps.getProps("hdfs.nameservices");//ns1
        conf.set("fs.defaultFS", "hdfs://" + nameservices);    //这些信息存储在env 环境变量中
        conf.set("dfs.nameservices", nameservices);//" String nameservices = ns1";
        conf.set("dfs.ha.namenodes." + nameservices, namenodes[0] + "," + namenodes[1]);
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0] + ":8020");
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1] + ":8020");
        conf.set("dfs.client.failover.proxy.provider." + nameservices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        String hdfsRPCUrl = "hdfs://" + nameservices + ":8020";     //HDFS_RPC_PORT=8020 也是从环境变量获取 或 dfs.namenode.rpc-address.cluster1.nn1
        dfs = (DistributedFileSystem) DistributedFileSystem.get(URI.create(hdfsRPCUrl), conf, userName);
        //dfs.initialize(URI.create(hdfsRPCUrl), conf);
    }

    /**
     * 上传本地文件到HDFS
     *
     * @param localDir 本地文件路径
     * @param hdfsDir hdfs文件夹路径名
     */
    public static void copyFromLocalFile(String localDir, String hdfsDir) throws Exception {
        if (null == dfs) {
            connHadoopByHA();
        }
        Path resP = new Path(localDir);
        Path destP = new Path(hdfsDir);
        if (!dfs.exists(destP)) {
            dfs.mkdirs(destP);
        }
        dfs.copyFromLocalFile(resP, destP);
        logger.info("上传数据到hdfs成功\t{}->{}", localDir, hdfsDir);
    }

    /**
     * 创建hdfs文件
     *
     * @param hdfsPath hdfs文件路径名
     * @return
     */
    public static boolean touch(String hdfsPath) {
        boolean touch = false;
        try {
            Path path = new Path(hdfsPath);
            if (dfs.exists(path)) {
                logger.info(hdfsPath + " 文件存在...");
            } else {
                touch = dfs.createNewFile(path);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        return touch;
    }

    /**
     * 向文件写入数据
     *
     * @param hdfsPath hdfs文件路径名
     * @param info     写入的信息
     * @return
     */
    public static boolean write(String hdfsPath, String info) {
        boolean write = false;
        FSDataOutputStream fsDataOutputStream = null;
        try {
            Path path = new Path(hdfsPath);
            if (dfs.exists(path)) {
                fsDataOutputStream = dfs.create(path);
                //fsDataOutputStream.writeUTF(info);
                fsDataOutputStream.writeBytes(info);
                fsDataOutputStream.flush();
                logger.info(hdfsPath + " 文件存在...");
            } else {
                logger.info(hdfsPath + " 文件不存在...");
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            try {
                fsDataOutputStream.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }
        return write;
    }

    /**
     * 关闭
     */
    public static void destory() throws IOException {
        if (dfs != null) {
            dfs.close();
        }
        logger.info("关闭hdfs资源");
    }
}
