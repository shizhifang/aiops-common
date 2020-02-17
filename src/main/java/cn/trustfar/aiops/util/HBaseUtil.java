package cn.trustfar.aiops.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtil {
    private static Logger logger = LoggerFactory.getLogger(HBaseUtil.class.getSimpleName());
    //    private static String path = "C:\\fanshengli\\work\\product\\kerboes\\conf_test57\\";
    private Admin admin = null;
    private Configuration conf = null;
    private Connection connection = null;
    private Table table = null;
    public static String zookeeperQuorum = null;//sparkPro.getProps("hbase.zookeeper.quorum");
    public static String zookeeperPort = null;//sparkPro.getProps("hbase.zookeeper.property.clientPort");

    /**
     * 检查用户是否初始化hbase,没有则创建连接
     * @param userNmae
     */
    public void checkActive(String userNmae) {
        if (null == conf || null == connection || null == admin) {
            logger.warn("未曾连接过hbase服务器，正在进行重新连接！！");
            init(userNmae);
        } else {
            logger.info("---------------hbase为连接成功状态------------------");
        }
    }
    /**
     * 检查是否初始化hbase,没有则创建连接
     */
    public void checkActive() {
        if (null == conf || null == connection || null == admin) {
            logger.warn("未曾连接过hbase服务器，正在进行重新连接！！");
            init();
        } else {
            logger.info("---------------hbase为连接成功状态------------------");
        }
    }

    /**
     * 用户初始化hbase的连接
     */
    public void init(String userNmae) {
        try {
//            System.setProperty("HADOOP_HOME","G:\\data\\hadoop\\package\\hadoop-3.1.2");
//            System.setProperty("HADOOP_USER_NAME", userNmae);
            logger.info("开始初始化hbase的连接");
            //kerberos认证配置
//            kerberosConfig();
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
            conf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            conf.set("hbase.client.retries.number", "3");//失败时重试次数
            conf.set("hbase.rpc.timeout", "3000");//如果某次RPC时间超过该值，客户端就会主动关闭socket
//            conf.set("hbase.client.operation.timeout", "3000");//为一次操作总的时间(从开始调用到重试n次之后失败的总时间)
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userNmae);
            User user = User.create(ugi);
            connection = ConnectionFactory.createConnection(conf, user);
            admin = connection.getAdmin();
            if (null != conf && null != connection && null != admin) {
                logger.info("---------------连接hbase成功------------------");
            } else {
                logger.warn("---------------连接hbase失败------------------");
            }
        } catch (Exception e) {
            logger.error("连接hbase server fail：{}", e.getMessage());
            e.printStackTrace();
        }
    }
    /**
     * 初始化hbase的连接
     */
    public void init() {
        try {
//            System.setProperty("HADOOP_HOME","G:\\data\\hadoop\\package\\hadoop-3.1.2");
//            System.setProperty("HADOOP_USER_NAME", userNmae);
            logger.info("开始初始化hbase的连接");
            //kerberos认证配置
//            kerberosConfig();
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
            conf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            conf.set("hbase.client.retries.number", "3");//失败时重试次数
            conf.set("hbase.rpc.timeout", "3000");//如果某次RPC时间超过该值，客户端就会主动关闭socket
//            conf.set("hbase.client.operation.timeout", "3000");//为一次操作总的时间(从开始调用到重试n次之后失败的总时间)
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            if (null != conf && null != connection && null != admin) {
                logger.info("---------------连接hbase成功------------------");
            } else {
                logger.warn("---------------连接hbase失败------------------");
            }
        } catch (Exception e) {
            logger.error("连接hbase server fail：{}", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     */
    public void destory() {
        try {
            if (table != null) {
                table.close();
            }
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public  List<String> qurryTableTestBatch(List<String> rowkeyList) throws IOException {
        List<String> resultList=new ArrayList<>();
        List<Get> getList = new ArrayList();
        String tableName = "table_a";
        Table table = connection.getTable( TableName.valueOf(tableName));// 获取表
        for (String rowkey : rowkeyList){//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        for (Result result : results){//对返回的结果集进行操作
            for (Cell kv : result.rawCells()) {
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                resultList.add(value);
            }
        }
        return resultList;
    }

}
