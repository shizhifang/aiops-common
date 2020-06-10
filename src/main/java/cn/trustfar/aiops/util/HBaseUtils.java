package cn.trustfar.aiops.util;


import cn.trustfar.aiops.pojo.PageRequest;
import cn.trustfar.aiops.pojo.PerfAlertDealData;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * HBase查询与插入操作工具类
 *
 * @author author
 *
 */
//采用注入方式，HBaseService为定义的查询接口，可不需要。

public class HBaseUtils {
    private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class.getSimpleName());

    private static ReadProps readProps = new ReadProps("commo_config.properties");
    private static final String HBASE_ZOOKEEPER_QUORUM = readProps.getProps("hbase.zookeeper.quorum");
    private static final String ZOOKEEPER_ZNODE_PARENT = readProps.getProps("hbase.zookeeper.property.clientPort");

    private static volatile Connection connection = null;
    private static volatile Configuration configuration=null;
    private static volatile Admin admin = null;

    private static HBaseUtils instance = null;

    static {
        if(connection == null) {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
            configuration.set("zookeeper.znode.parent", ZOOKEEPER_ZNODE_PARENT);
            configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
            configuration.set("hbase.client.retries.number", "3");
            configuration.set("hbase.rpc.timeout", "6000");
            try {
                connection = ConnectionFactory.createConnection(
                        configuration,
                        new ThreadPoolExecutor(
                                20,
                                100,
                                60,
                                TimeUnit.MINUTES,
                                new LinkedBlockingQueue<>(20),
                                new ThreadFactoryImpl("cn.trustfar.aiops.util")
                        )
                );
                admin = connection.getAdmin();
            } catch (IOException e) {
                logger.error("Create connection or admin error! " + e.getMessage(), e);
            }
        }
    }
    /**
     * 用于创建线程池。
     * ThreadFactory实现类：重命名线程
     * @see ThreadFactory
     */
    private static class ThreadFactoryImpl implements ThreadFactory {
        private final String name;
        private AtomicInteger id = new AtomicInteger(1);

        private ThreadFactoryImpl(String name){
            this.name = "线程名字:ThreadFactory-" + name + "-" + id.getAndIncrement();
        }

        @Override
        public Thread newThread(@Nonnull Runnable runnable) {
            return new Thread(runnable, name);
        }
    }
    //简单单例方法，如果autowired自动注入就不需要此方法
    public static synchronized HBaseUtils getInstance(){
        if(instance == null){
            instance = new HBaseUtils();
        }
        return instance;
    }
    /**
     * 利用协处理器进行全表count统计
     *
     * @param tablename
     */
    public Long countRowsWithCoprocessor(String tablename) throws Throwable {
        TableName name=TableName.valueOf(tablename);
        HTableDescriptor descriptor = admin.getTableDescriptor(name);

        String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        if (! descriptor.hasCoprocessor(coprocessorClass)) {
            admin.disableTable(name);
            descriptor.addCoprocessor(coprocessorClass);
            admin.modifyTable(name, descriptor);
            admin.enableTable(name);
        }

        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Scan scan = new Scan();
        AggregationClient aggregationClient = new AggregationClient(configuration);

        Long count = aggregationClient.rowCount(name, new LongColumnInterpreter(), scan);

        stopWatch.stop();
        System.out.println("RowCount：" + count +  "，全表count统计耗时：" + stopWatch.getSplitNanoTime());

        return count;
    }

    /**
     * 根据rowkey关键字查询报告记录
     *
     * @param tablename
     * @param rowKeyword
     * @return
     */
    public List scanReportDataByRowKeyword(String tablename, String rowKeyword) throws IOException {
        List list = new ArrayList<>();

        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();

        //添加行键过滤器，根据关键字匹配
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(rowKeyword));
        scan.setFilter(rowFilter);

        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                //TODO 此处根据业务来自定义实现
                list.add(null);
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return list;
    }
    /**
     *
     * @param map               es返回的集合
     * @param tableName         表名
     */
    @SneakyThrows
    public List<JSONObject> qurryTableHbaseBatch(String tableName, LinkedHashMap<String, Map<String, Object>> map) {
        List<JSONObject> resultList = new ArrayList<>();
        List<Get> getList = new ArrayList();
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        for (Map.Entry<String, Map<String, Object>> stringMapEntry : map.entrySet()) {
            Get get = new Get(Bytes.toBytes(stringMapEntry.getKey()));
            getList.add(get);
        }
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        for (Result result : results) {//对返回的结果集进行操作
            for (Cell kv : result.rawCells()) {
                byte[] qualifier = CellUtil.cloneQualifier(kv);
                String columnName = Bytes.toString(qualifier);
                if (!(columnName.equalsIgnoreCase("time") || columnName.equalsIgnoreCase("kpi_id") || columnName.equalsIgnoreCase("ci_id"))) {
                    String value = Bytes.toString(CellUtil.cloneValue(kv));
                    String rowKey = Bytes.toString(CellUtil.cloneRow(kv));
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("VALUE", value);
                    Map<String, Object> es = map.get(rowKey);
                    jsonObject.putAll(es);
                    resultList.add(jsonObject);
                }
            }
        }
        return resultList;
    }
    /**
     *
     * @param map               es返回的集合
     * @param tableName         表名
     */
    @SneakyThrows
    public List<JSONObject> qurryTableHbaseFilterBatch(String tableName, LinkedHashMap<String, Map<String, Object>> map, PageRequest<PerfAlertDealData> pageRequest) {
        List<JSONObject> resultList = new ArrayList<>();
        List<Get> getList = new ArrayList();

        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        for (Map.Entry<String, Map<String, Object>> stringMapEntry : map.entrySet()) {
            Get get = new Get(Bytes.toBytes(stringMapEntry.getKey()));
            getList.add(get);
        }
        int startMinute =DateUtil.getMinute(pageRequest.getRequest().getTimeStart());
        int endMinute =DateUtil.getMinute(pageRequest.getRequest().getTimeEnd());
        if(endMinute==0){
            endMinute=59;
        }
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        for (Result result : results) {//对返回的结果集进行操作
            for (Cell cell : result.rawCells()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                int hour=Integer.parseInt(rowKey.substring(rowKey.length()-4, rowKey.length()-2));
                Map<String, Object> es = map.get(rowKey);
                String monitor_time= String.valueOf(es.get("MONITOR_TIME")).substring(0,10);
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (!(columnName.equalsIgnoreCase("time") || columnName.equalsIgnoreCase("kpi_id") || columnName.equalsIgnoreCase("ci_id"))) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    if (pageRequest.getRequest().getTimeStart().indexOf(monitor_time)==0) {
                        if(Integer.parseInt(columnName)>=startMinute) {
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put("VALUE", value);
                            jsonObject.putAll(es);
                            resultList.add(jsonObject);
                        }
                   }else if(pageRequest.getRequest().getTimeEnd().indexOf(monitor_time)==0){
                        if(Integer.parseInt(columnName)<=endMinute) {
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put("VALUE", value);
                            jsonObject.putAll(es);
                            resultList.add(jsonObject);
                        }
                    }else{
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("VALUE", value);
                        jsonObject.putAll(es);
                        resultList.add(jsonObject);
                    }
                }
            }
        }
        return resultList;
    }

}