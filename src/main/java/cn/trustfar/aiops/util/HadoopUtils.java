package cn.trustfar.aiops.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HadoopUtils {
    private static ReadProps readProps = new ReadProps("commo_config.properties");
    private DistributedFileSystem dfs = null;//多namenode方式
    private Configuration conf = null;
    private static Logger logger = LoggerFactory.getLogger(HadoopUtils.class.getSimpleName());


    /**
     * 删除文件或者目录
     *
     * @param path 文件或者目录路径
     * @param flag true 删除目录，false删除文件
     * @return true成功，false失败
     * @throws Exception
     */
    public boolean removeFile(String path, boolean flag) throws Exception {
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
    public List<String> getFilePaths(String path, String pattern, boolean isName) throws Exception {
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
    public boolean isExit(String filepath) throws Exception {
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
     * 判断是否是目录
     *
     * @param srcPath
     * @return
     */
    public boolean isDirctory(String srcPath) throws Exception {
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
    public void connHadoopByHA() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs"); //针对hadoop，实际生效
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
//        dfs = (DistributedFileSystem) DistributedFileSystem.get(URI.create(hdfsRPCUrl), conf);
        dfs = (DistributedFileSystem) DistributedFileSystem.newInstance(URI.create(hdfsRPCUrl), conf);
        logger.info("******连接hdfs[{}]成功******", dfs.toString());
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
    public void connHadoopByHA(String userName) throws Exception {
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
     * @param hdfsDir  hdfs文件夹路径名
     */
    public void copyFromLocalFile(String localDir, String hdfsDir) throws Exception {
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
    public boolean touch(String hdfsPath) throws Exception {
        if (null == dfs) {
            connHadoopByHA();
        }
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
    public boolean write(String hdfsPath, String info,FSDataOutputStream fsDataOutputStream) throws Exception {
        boolean write = false;
        try {
            //fsDataOutputStream.writeUTF(info);
            fsDataOutputStream.writeBytes(info);
            fsDataOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
        return write;
    }

    /**
     * 把csv格式组成的list数据放入hdfs上面
     *
     * @param hdfsPath hdfs文件 example: /tmp/ci_id_time.csv
     * @param list     数据 每一条数据格式为按,切割
     * @param headers  csv文件的表头
     */
    public  void writeByList(String hdfsPath, List<String> list, String... headers) throws Exception {
        FSDataOutputStream fsDataOutputStream=null;
        try {
            if (null == dfs) {
                connHadoopByHA();
            }
            //判断hdfs文件是否存在
            Path path = new Path(hdfsPath);
            if (!dfs.exists(path)) {
                dfs.createNewFile(path);
            }
             fsDataOutputStream = dfs.create(path);
            //按照每4万条最多一个批次写入到hdfs中
            StringBuilder stringBuilder = new StringBuilder();
            if (null != headers) {
                if (headers.length == 1) {
                    stringBuilder.append(headers[0]).append("\n");
                } else {
                    for (int i = 0; i < headers.length; i++) {
                        if (i == headers.length - 1) {
                            stringBuilder.append(headers[i] + "\n");
                        } else {
                            stringBuilder.append(headers[i]).append(",");
                        }
                    }
                }
            }
            int size = list.size();
            int cache = 100000;
            int num = size / cache;
            //假设size=12  cache=4
            if (size > cache) {
                int before = 0;
                int tmp = 1;
                while (tmp <= num) {
                    //第一次int i=0 i<4，i++ tmp=1 cache=4 num=3
                    //第二次int i=4,i<8,i++  tmp=2 cache=4 num=3
                    //第二次int i=8,i<12,i++ tmp=3 cache=4 num=3
                    for (int i = before; i < cache * tmp; i++) {
                        stringBuilder.append(list.get(i) + "\n");
                    }
                    write(hdfsPath, stringBuilder.toString(),fsDataOutputStream);
                    before = cache * tmp;
                    tmp++;
                    stringBuilder.setLength(0);
                }
                if (size % cache != 0) {
                    //如果size=14 ，size%cache不等于0 还有在增加一次
                    //
                    // i=(cache=4*num=3);i<14;i++
                    for (int i = cache * num; i < size; i++) {
                        stringBuilder.append(list.get(i) + "\n");
                    }
                    write(hdfsPath, stringBuilder.toString(),fsDataOutputStream);
                    stringBuilder.setLength(0);
                }
            } else {//size<cache
                for (String s : list) {
                    stringBuilder.append(s + "\n");
                }
                write(hdfsPath, stringBuilder.toString(),fsDataOutputStream);
            }
        } catch (Exception e) {
            logger.error(e.toString());
            e.printStackTrace();
        } finally {
            if(null !=fsDataOutputStream){
                fsDataOutputStream.close();
            }
        }

    }

    public static void main(String[] args) {
       for(int i=1;i<3;i++){
           Thread thread1 = new Thread(new Runnable() {
               @Override
               public void run() {
                   HadoopUtils hadoopUtils=new HadoopUtils();
                   logger.info(Thread.currentThread().getName()+"\t在线程中2秒后将修改值");
                   try {
                       hadoopUtils.connHadoopByHA();
                   } catch (Exception e) {
                       e.printStackTrace();
                   }
               }
           });
           thread1.start();
       }

//        System.out.println(12 / 4);
//        List<String> list = new ArrayList<>();
//        list.add("aaaaaa,ddddddd");
//        list.add("cadcas,xxxxx");
//        list.add("ccccccc,safdsa");
//        StringBuilder stringBuilder = new StringBuilder();
//        for (String s : list) {
//            stringBuilder.append(s + "\n");
//        }
//        System.out.println(stringBuilder.toString());
//        System.out.println(list.toString());
    }

    /**
     * 关闭
     */
    public void destory() throws IOException {
        if (dfs != null) {
            dfs.close();
        }
        logger.info("关闭hdfs资源");
    }
}
