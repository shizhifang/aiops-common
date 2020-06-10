package cn.trustfar.aiops.util;

import cn.trustfar.aiops.bean.AlertResultBean;
import cn.trustfar.aiops.bean.CommonReustBean;
import cn.trustfar.aiops.bean.DealReulstBean;
import cn.trustfar.aiops.bean.PerfReusltBean;
import cn.trustfar.aiops.constant.CommonConstants;
import cn.trustfar.aiops.pojo.PageRequest;
import cn.trustfar.aiops.pojo.Parameter;
import cn.trustfar.aiops.pojo.PerfAlertDealData;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lyh
 * Es的连接工具,
 * 性能，交易，告警数据的索引库名字分别为perf_index，deal_index，alert_index
 */
public class EsUtils implements Serializable {
    private TransportClient client = null;

    public EsUtils() {
    }

    public EsUtils(String esName, String esIp) {
        checkinitClient(esName, esIp);
    }

    /**
     * 初始化es的客户端连接工具
     *
     * @param esName es集群的名字trustfar-elastic
     * @param esIp   es的ip地址"192.168.10.104"
     * @return
     */
    public TransportClient checkinitClient(String esName, String esIp) {
        if (client == null) {
            try {
                Settings settings = Settings.builder()
                        .put("cluster.name", esName)
                        //开启嗅探机制，自动发现es集群的服务器
//                        .put("client.transport.sniff", "false")
                        .build();

                client = new PreBuiltTransportClient(settings);
                String[] esIps = esIp.split(",");
                for (String ip : esIps) {
                    client.addTransportAddresses(new TransportAddress(InetAddress.getByName(ip), 9300));
                }
                //设置es集群的最大返回值2147483647
                setReturnDataByMaxNumber(esIps[0]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return client;
    }

    /**
     * 批量把json数据插入es
     * 6.4版本index下面只能有一个type
     *
     * @param jsonDatas
     * @param indexName es的索引库名必须是小写英文字母不能以下划线开头
     * @return
     * @throws UnknownHostException
     */
    public boolean batchInsert(TransportClient client, List<String> jsonDatas, String indexName) throws UnknownHostException {
        System.out.println("将要插入es的数据量-----------》" + jsonDatas.size());
        if (jsonDatas.size() == 0) {
            System.out.println("此批次无es数据插入");
            return false;
        }
        BulkRequestBuilder bulk = client.prepareBulk();
        for (String jsonData : jsonDatas) {
            bulk.add(buildIndex(client, jsonData, indexName, CommonConstants.EsIndexType));
        }
        BulkResponse bulkItemResponses = bulk.execute().actionGet();
        RestStatus status = bulkItemResponses.status();
        System.out.println(status);
        return "OK".equals(status.toString());
    }

    /**
     * es数据库添加一条json数据
     * 6.4版本index下面只能有一个type
     *
     * @param jsonData  要添加的数据
     * @param indexName es索引库名
     * @param typeName  es索引库表名
     * @return
     */
    public IndexRequestBuilder buildIndex(TransportClient client, String jsonData, String indexName, String typeName) {
        return client.prepareIndex(indexName, typeName).setSource(jsonData, XContentType.JSON);
    }

    /**
     * 创建es的索引库
     *
     * @param indexName
     */
    public void createIndex(String indexName) {
        client.admin().indices().prepareCreate(indexName).get();
    }

    /**
     * 删除索引
     *
     * @param indexName
     */
    public void deleteIndex(String indexName) {
        client.admin().indices().prepareDelete(indexName).get();
    }

    /**
     * @param list     具体查看Parameter
     * @param dataType 1,2,3分别为性能、告警、交易
     * @return
     */
    private SearchHit[] searchHits(List<Parameter> list, int dataType) {
        //1.创建多条件过滤查询器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //2.
        for (Parameter parameter : list) {
            //text字段类型精确查询需要用".keyword";
            String name = parameter.getName();
            List<Object> values = parameter.getValue();
            String valueType = parameter.getValueType();
            //3.根据值得关系创建不同过滤器
            if (valueType.equalsIgnoreCase(CommonConstants.OR)) {
                if (null != values) {
                    if (values.size() == 1) {
                        TermQueryBuilder builder = QueryBuilders.termQuery(name, values.get(0));
                        //每一个条件的过滤加入到多条件查询中
                        boolQueryBuilder.must(builder);
                    } else {
                        TermsQueryBuilder builder = QueryBuilders.termsQuery(name, values);
                        //每一个条件的过滤加入到多条件查询中
                        boolQueryBuilder.must(builder);
                    }
                }
            } else if (valueType.equalsIgnoreCase(CommonConstants.RANGE)) {
                if (null != values) {
                    RangeQueryBuilder builder = QueryBuilders.rangeQuery(name).gte(values.get(0)).lte(values.get(1));
                    //每一个条件的过滤加入到多条件查询中
                    boolQueryBuilder.must(builder);
                }
            } else if (valueType.equalsIgnoreCase(CommonConstants.SINGLE)) {
                if (null != values) {
 //                   TermQueryBuilder builder = QueryBuilders.termQuery(name, values.get(0));
                    //TODO termQuery的方式对一大串的目前查询不到，暂时先用match
                    MatchQueryBuilder builder = QueryBuilders.matchQuery(name, values.get(0)).operator(Operator.AND);
                    //每一个条件的过滤加入到多条件查询中
                    boolQueryBuilder.must(builder);
                }
            }
        }
        //4.查询解析不同的es表，性能表，交易表，告警表
        String esIndex = getEsIndex(dataType);
        //按照时间字段排序
//        FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort("MONITOR_TIME");
        //5.client访问es返回hits
        SearchResponse searchResponse = client.prepareSearch(esIndex)
                .setTypes(CommonConstants.EsIndexType)
                .setQuery(boolQueryBuilder)
//                .addSort(fieldSortBuilder)
                .setSize(2147483647)//Elasticsearch支持的最大值是2^31-1
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        return hits1;
    }

    /**
     * 通过条件查询es获取rowkey列表
     * @param list 具体查看Parameter
     * @param dataType  1,2,3分别为性能、告警、交易
     * @return
     */
    private List<String> searchRowKeysByEs(List<Parameter> list, int dataType){
        SearchHit[] searchHits = searchHits(list, dataType);
        Map<String,Integer> map=new HashMap();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String rowKey = String.valueOf(sourceAsMap.get("rowKey"));
            map.put(rowKey,1);
        }
        List<String> rowKeys=new ArrayList<>(map.keySet());
        return rowKeys;
    }
    /**
     * 通过条件查询es获取rowkey列表
     * @param list 具体查看Parameter
     * @param dataType  1,2,3分别为性能、告警、交易
     * @return
     */
    private List<String> searchRowKeysByOneEs(List<Parameter> list, int dataType){
        SearchHit[] searchHits = searchHits(list, dataType);
        Map<String,Integer> map=new HashMap();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String rowKey = String.valueOf(sourceAsMap.get("rowKey"));
            map.put(rowKey,1);
        }
        List<String> rowKeys=new ArrayList<>(map.keySet());
        return rowKeys;
    }

    /**
     * 根据不同数据类型（性能，交易，告警）查询数据
     *
     * @param parameters    查看Parameter类
     * @param dataType      CommonConstants.PERFORMANCE.AlERT.DEAL
     * @param sendFrequency 数据发送的频率,用于缺失值计算
     * @return 返回time, value按, 分割的list集合
     * example: 2019-01-03 22:11:00,1234
     */
    public List<String> getListTimeAndValue(List<Parameter> parameters,
                                            int dataType,
                                            int sendFrequency) throws ParseException {
        SearchHit[] searchHits = searchHits(parameters, dataType);
        List<String> resultList = new ArrayList<>();//最终返回的数据
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            //格式年月日时分秒20190123100908
            String time = String.valueOf(sourceAsMap.get("MONITOR_TIME"));
            //转换成分钟格式年月日时分20190123100900
//            String secondTime = time.toString().substring(0, time.toString().length() - 2)+"00";
            String timeString = TimeUtils.getStringTimeByStringTime(time);
            if (StringUtils.isNotEmpty(timeString)) {
                String value = String.valueOf(sourceAsMap.get("VALUE"));
                if (value.equalsIgnoreCase("0x0000") || value.equalsIgnoreCase("0x0800")) {
                    resultList.add(timeString + "," + "1");
                } else {
                    resultList.add(timeString + "," + value);
                }
            }
        }
        //TODO缺失值计算
        int missFrequency = sendFrequency * CommonConstants.Unit_five;
        return resultList;
    }

    /**
     * 异常定位查询到的数据ci_id,kpi_id,time,value放到hdfs上面
     *
     * @param parameters 查看Parameter类
     * @param dataType   CommonConstants.PERFORMANCE.AlERT.DEAL
     * @param hdfsPath   example:
     *                   /tmp/aaa.csv
     * @return true有数据并且放到hdfs上面，false无数据
     * @throws Exception
     */
    public boolean putPositioningData2Hdfs(List<Parameter> parameters,
                                           int dataType,
                                           String hdfsPath) throws Exception {
        //1.获取异常定位信息
        boolean result = false;
        List<String> resultList = getPositioningDatas(parameters, dataType);
//        2.上传数据到hdfs
        HadoopUtils hadoopUtils = new HadoopUtils();
        if (resultList.size() > 0) {
            hadoopUtils.writeByList(hdfsPath, resultList, "ci_id", "kpi_id", "time", "value");
            hadoopUtils.destory();
            result = true;
        }
        return result;
    }


    /**
     * 根据不同数据类型（性能，交易，告警）查询数据
     *
     * @param list          查看Parameter类
     * @param dataType      CommonConstants.PERFORMANCE.AlERT.DEAL
     * @param sendFrequency 数据发送的频率,用于缺失值计算
     * @return 返回time, value按, 分割的list集合
     * example: Map<key,value>key=CI_ID,value=2019-01-03 22:11:00,1234
     * @throws ParseException
     */
    public Map<String, List<String>> getMapTimeAndValueData(List<Parameter> list,
                                                            int dataType,
                                                            int sendFrequency) throws ParseException {
        SearchHit[] searchHits = searchHits(list, dataType);
        List<String> resultList = new ArrayList<>();//最终返回的数据
        Map<String, List<String>> resultMap = new LinkedHashMap<>();
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            //格式年月日时分秒20190123100908
            String time = String.valueOf(sourceAsMap.get("MONITOR_TIME"));
            String ci_id = String.valueOf(sourceAsMap.get("CI_ID"));
            //转换成分钟格式年月日时分2019-01-23 10:09:00
            String timeString = TimeUtils.getStringTimeByStringTime(time);
            String value = String.valueOf(sourceAsMap.get("VALUE"));
            if (StringUtils.isNotEmpty(timeString)) {
                if (value.equalsIgnoreCase("0x0000") || value.equalsIgnoreCase("0x0800")) {
                    resultList.add(timeString + "," + "1");
                } else {
                    resultList.add(timeString + "," + value);
                }
            }
            resultMap.put(ci_id, resultList);
        }
        //TODO缺失值计算
        int missFrequency = sendFrequency * CommonConstants.Unit_five;
        return resultMap;

    }

    /**
     * 根据不同ci_id和过滤条件查询性能或者交易或者告警数据，把每一个相同的ci_id数据放在一个hdfs文件上面
     *
     * @param parameters    查看Parameter类
     * @param dataType      CommonConstants.PERFORMANCE.AlERT.DEAL
     * @param sendFrequency 数据发送的频率,用于缺失值计算 CommonConstants.FrequencyMinute
     * @return hdfs路径的list集合/tmp/ci_id.csv 数据的表头为time,alue
     * @throws Exception
     */
    public List<String> putMoreCiData2Hdfs(List<Parameter> parameters,
                                           int dataType,
                                           int sendFrequency) throws Exception {
        List<String> reslutList = new ArrayList<>();
        HadoopUtils hadoopUtils = new HadoopUtils();
        Map<String, List<String>> moreCiIdData = getMapTimeAndValueData(parameters, dataType, sendFrequency);
        for (Map.Entry<String, List<String>> stringListEntry : moreCiIdData.entrySet()) {
            String key = stringListEntry.getKey();
            String hdfsPath = getHdfsPath(key);
            List<String> value = stringListEntry.getValue();
            hadoopUtils.writeByList(hdfsPath, value, "time", "value");
            reslutList.add(hdfsPath);
        }
        hadoopUtils.destory();
        return reslutList;
    }

    /**
     * 根据CI_ID+秒级时间构建hdfs路径
     *
     * @param CI_ID
     * @return
     */
    private String getHdfsPath(String CI_ID) {
        Random random = new Random();
        int randomValue = random.nextInt(10000) + 10000;
        String path = "/tmp/" + CI_ID + "_" + TimeUtils.getSystimeMsTime() + ".csv";
        return path;
    }

    /**
     * @param list
     * @param dataType
     * @param sendFrequency
     * @return 数据的表头为time, alue
     * @throws Exception
     */
    public Map<String, String> getCiDataHdfsFile(List<Parameter> list,
                                                 int dataType,
                                                 int sendFrequency) throws Exception {
        Map<String, String> reslutMap = new HashMap<>();
        HadoopUtils hadoopUtils = new HadoopUtils();
        Map<String, List<String>> moreCiIdData = getMapTimeAndValueData(list, dataType, sendFrequency);
        for (Map.Entry<String, List<String>> stringListEntry : moreCiIdData.entrySet()) {
            String key = stringListEntry.getKey();
            String hdfsPath = getHdfsPath(key);
            List<String> value = stringListEntry.getValue();
            hadoopUtils.writeByList(hdfsPath, value, "time", "value");
            reslutMap.put(key, hdfsPath);
        }
        hadoopUtils.destory();
        return reslutMap;
    }

    public Map<String, List<String>> getCiData(List<Parameter> list,
                                               int dataType,
                                               int sendFrequency) throws Exception {
        return getMapTimeAndValueData(list, dataType, sendFrequency);
    }

    private void createQueryBuilderByPerfAlertDeal(PerfAlertDealData perfAlertDealData, BoolQueryBuilder boolQueryBuilder) {
        List<String> ci_id_list = perfAlertDealData.getCI_ID_LIST();
        String timeStart = perfAlertDealData.getTimeStart();
        String timeEnd = perfAlertDealData.getTimeEnd();
        String monitor = perfAlertDealData.getMonitor();
        String module = perfAlertDealData.getModule();
        String system = perfAlertDealData.getSystem();
        String kpiCode = perfAlertDealData.getKpiCode();
        String kpiId = perfAlertDealData.getKpiId();
        String kpiName = perfAlertDealData.getKpiName();
        String kpiShortName = perfAlertDealData.getKpiShortName();
        String alertLevel = perfAlertDealData.getAlertLevel();
        //1.获取要过滤的字段放入map中 key值为过滤的字段名字，value为过滤字段名字的值
        Map<String, String> map = new HashMap<>();
        if (StringUtils.isNotEmpty(monitor)) {
            map.put("MONITOR", monitor);
        }
        if (StringUtils.isNotEmpty(system)) {
            map.put("SYSTEM", system);
        }
        if (StringUtils.isNotEmpty(module)) {
            map.put("MODULE", module);
        }
        if (StringUtils.isNotEmpty(kpiId)) {
            map.put("KPI_ID", kpiId);
        }
        if (StringUtils.isNotEmpty(kpiCode)) {
            map.put("KPI_CODE", kpiCode);
        }
        if (StringUtils.isNotEmpty(kpiName)) {
            map.put("KPI_NAME", kpiName);
        }
        if (StringUtils.isNotEmpty(kpiShortName)) {
            map.put("KPI_SHORT_NAME", kpiShortName);
        }
        if (StringUtils.isNotEmpty(alertLevel)) {
            map.put("ALERT_LEVEL", alertLevel);
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isNotBlank(value)) {
                TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(key, value);
                boolQueryBuilder.must(termQueryBuilder);
            }
        }
        //2.添加多个CI_ID过滤项
        if (null != ci_id_list) {
            TermsQueryBuilder queryBuilder = QueryBuilders.termsQuery("CI_ID", ci_id_list);
            boolQueryBuilder.must(queryBuilder);
        }
        //3.添加时间的查询
        if (StringUtils.isNotBlank(timeEnd)) {
            RangeQueryBuilder monitor_time = QueryBuilders.rangeQuery("MONITOR_TIME").lte(timeEnd);
            boolQueryBuilder.must(monitor_time);
        }
        if (StringUtils.isNotBlank(timeStart)) {
            RangeQueryBuilder monitor_time = QueryBuilders.rangeQuery("MONITOR_TIME").gte(timeStart);
            boolQueryBuilder.must(monitor_time);
        }
    }

    /**
     * @param dataType          性能1、告警2、交易3
     * @param pageRequest       分页对象
     * @return
     */
    private SearchHits searchByPerfAlertDealData( int dataType, PageRequest<PerfAlertDealData> pageRequest) {
        //1.创建多条件过滤查询器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //2.添加过滤条件
        createQueryBuilderByPerfAlertDeal(pageRequest.getRequest(), boolQueryBuilder);
        //3.查询解析不同的es表，性能表，交易表，告警表
        String indexName = getEsIndex(dataType);
        //4.设置分页默认20
        if (pageRequest.getPageNo() == null || pageRequest.getPageNo()==0) {
            pageRequest.setPageNo(1);
        }
        if (pageRequest.getPageSize() == null) {
            pageRequest.setPageSize(20);
        }
        //5.client访问es返回hits
        FieldSortBuilder sortBuilder = new FieldSortBuilder("MONITOR_TIME").order(SortOrder.DESC);
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(CommonConstants.EsIndexType)
                .setQuery(boolQueryBuilder)
                .setFrom((pageRequest.getPageNo() - 1)*pageRequest.getPageSize())
                .setSize(pageRequest.getPageSize())
                .addSort(sortBuilder)
                .get();
        SearchHits hits = searchResponse.getHits();
        return hits;
    }

    /**
     * @param dataType          性能1、告警2、交易3
     * @param pageRequest       分页对象
     * @return
     */
    @SneakyThrows
    private SearchHits searchEsByPerfAlertDealData(int dataType, PageRequest<PerfAlertDealData> pageRequest, RestHighLevelClient restHighLevelClient) {
        //1.创建多条件过滤查询器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //2.添加过滤条件
        createQueryBuilderByPerfAlertDeal(pageRequest.getRequest(), boolQueryBuilder);
        //3.查询解析不同的es表，性能表，交易表，告警表
        String indexName = getEsIndex(dataType);
        //4.设置分页默认20
        if (pageRequest.getPageNo() == null || pageRequest.getPageNo()==0) {
            pageRequest.setPageNo(1);
        }
        if (pageRequest.getPageSize() == null) {
            pageRequest.setPageSize(20);
        }
        //5.client访问es返回hits
        FieldSortBuilder sortBuilder = new FieldSortBuilder("MONITOR_TIME").order(SortOrder.DESC);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from((pageRequest.getPageNo() - 1)*pageRequest.getPageSize());
        sourceBuilder.size(pageRequest.getPageSize());
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(CommonConstants.EsIndexType);
        searchRequest.source(sourceBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.sort(sortBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        return hits;
    }

    /**
     * 根据查询到的数据，获取性能、交易、告警接口根据条件查询需要返回的字段和值并放入json中
     *
     * @param searchHits es查询后返回的所有数据
     * @param dataType   查看CommonConstants类
     * @return
     */
    private static List<JSONObject> getJsonListByDataType(SearchHit[] searchHits, int dataType) {
        List<JSONObject> resultList = new ArrayList<>();
        if (dataType == CommonConstants.PERFORMANCE) {
            PerfReusltBean perfReusltBean = new PerfReusltBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                JSONObject jsonObject = new JSONObject();
                addCommonReustBean2JsonList(sourceAsMap, jsonObject);
                //TODO 返回值可能会更改的地方
                jsonObject.put(perfReusltBean.KPI_UNIT, sourceAsMap.get(perfReusltBean.KPI_UNIT));
                jsonObject.put(perfReusltBean.KPI_VAL_TYPE, sourceAsMap.get(perfReusltBean.KPI_VAL_TYPE));
                jsonObject.put(perfReusltBean.TIME, sourceAsMap.get(perfReusltBean.TIME));
                jsonObject.put(perfReusltBean.CI_ID, sourceAsMap.get(perfReusltBean.CI_ID));


                jsonObject.put(perfReusltBean.VALUE, sourceAsMap.get(perfReusltBean.VALUE));



                resultList.add(jsonObject);
            }

        } else if (dataType == CommonConstants.AlERT) {
            AlertResultBean alertResultBean = new AlertResultBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                JSONObject jsonObject = new JSONObject();
                addCommonReustBean2JsonList(sourceAsMap, jsonObject);
                //TODO 返回值可能会更改的地方
                jsonObject.put(alertResultBean.ALERT_KEY, sourceAsMap.get(alertResultBean.ALERT_KEY));
                jsonObject.put(alertResultBean.ALERT_LEVEL, sourceAsMap.get(alertResultBean.ALERT_LEVEL));
                jsonObject.put(alertResultBean.ALERT_STATE, sourceAsMap.get(alertResultBean.ALERT_STATE));
                jsonObject.put(alertResultBean.CI_ID, sourceAsMap.get(alertResultBean.CI_ID));
                jsonObject.put(alertResultBean.VALUE, sourceAsMap.get(alertResultBean.VALUE));
                jsonObject.put(alertResultBean.ALERT_TIME, sourceAsMap.get(alertResultBean.ALERT_TIME));
                jsonObject.put(alertResultBean.ALERT_SUMMARY, sourceAsMap.get(alertResultBean.ALERT_SUMMARY));
                jsonObject.put(alertResultBean.DEV_CODE, sourceAsMap.get(alertResultBean.DEV_CODE));
                jsonObject.put(alertResultBean.NODE, sourceAsMap.get(alertResultBean.NODE));
                jsonObject.put(alertResultBean.ALERT_TYPE, sourceAsMap.get(alertResultBean.ALERT_TYPE));
                resultList.add(jsonObject);
            }
        } else if (dataType == CommonConstants.DEAL) {
            DealReulstBean dealReulstBean = new DealReulstBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                JSONObject jsonObject = new JSONObject();
                addCommonReustBean2JsonList(sourceAsMap, jsonObject);
                //TODO 返回值可能会更改的地方
                jsonObject.put(dealReulstBean.KPI_UNIT, sourceAsMap.get(dealReulstBean.KPI_UNIT));
                jsonObject.put(dealReulstBean.KPI_VAL_TYPE, sourceAsMap.get(dealReulstBean.KPI_VAL_TYPE));
                jsonObject.put(dealReulstBean.TIME, sourceAsMap.get(dealReulstBean.TIME));
                jsonObject.put(dealReulstBean.CI_ID, sourceAsMap.get(dealReulstBean.CI_ID));
                jsonObject.put(dealReulstBean.VALUE, sourceAsMap.get(dealReulstBean.VALUE));
                resultList.add(jsonObject);
            }
        }
        return resultList;
    }

    /**
     * 根据查询到的数据，获取性能、交易、告警接口根据条件查询需要返回的字段和值并放入json中
     * 一对一查询 实现跨分钟查询
     * @param searchHits es查询后返回的所有数据
     * @param dataType   查看CommonConstants类
     * @param pageRequest
     * @return
     */
    @SneakyThrows
    private static List<JSONObject> getJsonListByEsOneToHbaseFilterDataType(SearchHit[] searchHits, int dataType, PageRequest<PerfAlertDealData> pageRequest) {
        List<JSONObject> resultList = new ArrayList<>();
        LinkedHashMap<String,Map<String,Object>> linkedHashMap=new LinkedHashMap<>();
        Map<String,Object> stringObjectMap=null;
        JSONObject jsonObject=null;
        if (dataType == CommonConstants.PERFORMANCE) {
            PerfReusltBean perfReusltBean = new PerfReusltBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                jsonObject = new JSONObject();
                stringObjectMap=new HashMap<>();
                addCommonReustBean2MapList(sourceAsMap, stringObjectMap);
                //TODO 返回值可能会更改的地方
                stringObjectMap.put(perfReusltBean.KPI_UNIT, sourceAsMap.get(perfReusltBean.KPI_UNIT));
                stringObjectMap.put(perfReusltBean.KPI_VAL_TYPE, sourceAsMap.get(perfReusltBean.KPI_VAL_TYPE));
                stringObjectMap.put(perfReusltBean.TIME, sourceAsMap.get(perfReusltBean.TIME));
                stringObjectMap.put(perfReusltBean.CI_ID, sourceAsMap.get(perfReusltBean.CI_ID));
                linkedHashMap.put(String.valueOf(sourceAsMap.get("rowKey")),stringObjectMap);
            }
            HBaseUtils hBaseUtils = new HBaseUtils();
//            HBaseUtil hBaseUtil=new HBaseUtil();
//            hBaseUtil.checkActive();
            resultList = hBaseUtils.qurryTableHbaseFilterBatch("aiops:perform",linkedHashMap,pageRequest);
        } else if (dataType == CommonConstants.AlERT) {
            AlertResultBean alertResultBean = new AlertResultBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                jsonObject = new JSONObject();
                addCommonReustBean2JsonList(sourceAsMap, jsonObject);
                //TODO 返回值可能会更改的地方
                jsonObject.put(alertResultBean.ALERT_KEY, sourceAsMap.get(alertResultBean.ALERT_KEY));
                jsonObject.put(alertResultBean.ALERT_LEVEL, sourceAsMap.get(alertResultBean.ALERT_LEVEL));
                jsonObject.put(alertResultBean.ALERT_STATE, sourceAsMap.get(alertResultBean.ALERT_STATE));
                jsonObject.put(alertResultBean.CI_ID, sourceAsMap.get(alertResultBean.CI_ID));
                jsonObject.put(alertResultBean.VALUE, sourceAsMap.get(alertResultBean.VALUE));
                jsonObject.put(alertResultBean.ALERT_TIME, sourceAsMap.get(alertResultBean.ALERT_TIME));
                jsonObject.put(alertResultBean.ALERT_SUMMARY, sourceAsMap.get(alertResultBean.ALERT_SUMMARY));
                jsonObject.put(alertResultBean.DEV_CODE, sourceAsMap.get(alertResultBean.DEV_CODE));
                jsonObject.put(alertResultBean.NODE, sourceAsMap.get(alertResultBean.NODE));
                jsonObject.put(alertResultBean.ALERT_TYPE, sourceAsMap.get(alertResultBean.ALERT_TYPE));
                // linkedHashMap.put(String.valueOf(sourceAsMap.get("rowKey")),stringObjectMap);

                resultList.add(jsonObject);
            }
        } else if (dataType == CommonConstants.DEAL) {
            DealReulstBean dealReulstBean = new DealReulstBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                jsonObject = new JSONObject();
                stringObjectMap=new HashMap<>();
                addCommonReustBean2MapList(sourceAsMap, stringObjectMap);
                //TODO 返回值可能会更改的地方
                stringObjectMap.put(dealReulstBean.KPI_UNIT, sourceAsMap.get(dealReulstBean.KPI_UNIT));
                stringObjectMap.put(dealReulstBean.KPI_VAL_TYPE, sourceAsMap.get(dealReulstBean.KPI_VAL_TYPE));
                stringObjectMap.put(dealReulstBean.TIME, sourceAsMap.get(dealReulstBean.TIME));
                stringObjectMap.put(dealReulstBean.CI_ID, sourceAsMap.get(dealReulstBean.CI_ID));
                linkedHashMap.put(String.valueOf(sourceAsMap.get("rowKey")),stringObjectMap);

            }
//            HBaseUtil hBaseUtil=new HBaseUtil();
//            hBaseUtil.checkActive();
//            resultList = hBaseUtil.qurryTableHbaseBatch("aiops:deal",linkedHashMap);
//

            HBaseUtils hBaseUtils = new HBaseUtils();
//            HBaseUtil hBaseUtil=new HBaseUtil();
//            hBaseUtil.checkActive();
            resultList = hBaseUtils.qurryTableHbaseFilterBatch("aiops:perform",linkedHashMap,pageRequest);

        }
        return resultList;
    }
    /**
     * 性能，交易，告警共用的字段放到返回的JSONObject中
     *
     * @param sourceAsMap 通过es查询之后的searchHit.getSourceAsMap()得到
     * @param stringObjectMap
     */
    private static void addCommonReustBean2MapList(Map<String, Object> sourceAsMap, Map<String,Object> stringObjectMap) {
        CommonReustBean commonReustBean = new CommonReustBean();
        stringObjectMap.put(commonReustBean.KPI_ID, sourceAsMap.get(commonReustBean.KPI_ID));
        stringObjectMap.put(commonReustBean.KPI_CODE, sourceAsMap.get(commonReustBean.KPI_CODE));
        stringObjectMap.put(commonReustBean.KPI_NAME, sourceAsMap.get(commonReustBean.KPI_NAME));
        stringObjectMap.put(commonReustBean.KPI_SHORT_NAME, sourceAsMap.get(commonReustBean.KPI_SHORT_NAME));
        stringObjectMap.put(commonReustBean.MODULE, sourceAsMap.get(commonReustBean.MODULE));
        stringObjectMap.put(commonReustBean.MONITOR, sourceAsMap.get(commonReustBean.MONITOR));
        stringObjectMap.put(commonReustBean.SYSTEM, sourceAsMap.get(commonReustBean.SYSTEM));
    }
    /**
     * 性能，交易，告警共用的字段放到返回的JSONObject中
     *
     * @param sourceAsMap 通过es查询之后的searchHit.getSourceAsMap()得到
     * @param jsonObject
     */
    private static void addCommonReustBean2JsonList(Map<String, Object> sourceAsMap, JSONObject jsonObject) {
        CommonReustBean commonReustBean = new CommonReustBean();
        jsonObject.put(commonReustBean.KPI_ID, sourceAsMap.get(commonReustBean.KPI_ID));
        jsonObject.put(commonReustBean.KPI_CODE, sourceAsMap.get(commonReustBean.KPI_CODE));
        jsonObject.put(commonReustBean.KPI_NAME, sourceAsMap.get(commonReustBean.KPI_NAME));
        jsonObject.put(commonReustBean.KPI_SHORT_NAME, sourceAsMap.get(commonReustBean.KPI_SHORT_NAME));
        jsonObject.put(commonReustBean.MODULE, sourceAsMap.get(commonReustBean.MODULE));
        jsonObject.put(commonReustBean.MONITOR, sourceAsMap.get(commonReustBean.MONITOR));
        jsonObject.put(commonReustBean.SYSTEM, sourceAsMap.get(commonReustBean.SYSTEM));
    }

    /**
     * 离线数据接口，根据提供的条件查询性能数据
     *
     * @param pageRequest
     * @return
     */
    public PageResultBean<List<JSONObject>> searchPerf(PageRequest<PerfAlertDealData> pageRequest) {

        SearchHits hits = searchByPerfAlertDealData( CommonConstants.PERFORMANCE, pageRequest);

        SearchHit[] searchHits = hits.getHits();

       // List<JSONObject> result = getJsonListByEsOneToHbaseDataType(searchHits, CommonConstants.PERFORMANCE);
        List<JSONObject> result = getJsonListByEsOneToHbaseFilterDataType(searchHits, CommonConstants.PERFORMANCE,pageRequest);


        return setPageResultOneToOneBean(hits,result,pageRequest);
    }

    /**
     * 离线数据接口，根据提供的条件查询性能数据
     *
     * @param pageRequest
     * @return
     */
    public PageResultBean<List<JSONObject>> searchEsHbasePerf(PageRequest<PerfAlertDealData> pageRequest, RestHighLevelClient restHighLevelClient) {

        SearchHits hits = searchEsByPerfAlertDealData( CommonConstants.PERFORMANCE, pageRequest,restHighLevelClient);

        SearchHit[] searchHits = hits.getHits();

        List<JSONObject> result = getJsonListByEsOneToHbaseFilterDataType(searchHits, CommonConstants.PERFORMANCE,pageRequest);
        return setPageResultOneToOneBean(hits,result,pageRequest);
    }

    /**
     * 离线数据接口，根据提供的条件查询交易数据
     *
     * @param pageRequest
     * @return
     */
    public PageResultBean<List<JSONObject>> searchDeal(PageRequest<PerfAlertDealData> pageRequest) {
        SearchHits hits = searchByPerfAlertDealData(CommonConstants.DEAL, pageRequest);
        SearchHit[] searchHits = hits.getHits();
        List<JSONObject> result = getJsonListByEsOneToHbaseFilterDataType(searchHits, CommonConstants.PERFORMANCE,pageRequest);
        return setPageResultOneToOneBean(hits,result,pageRequest);
    }

    /**
     * 离线数据接口，根据提供的条件查询alert数据
     *
     * @param pageRequest
     * @return
     */
    public PageResultBean<List<JSONObject>> searchAlert( PageRequest<PerfAlertDealData> pageRequest) {
        SearchHits hits = searchByPerfAlertDealData( CommonConstants.AlERT, pageRequest);
        SearchHit[] searchHits = hits.getHits();
        List<JSONObject> result = getJsonListByEsOneToHbaseFilterDataType(searchHits, CommonConstants.PERFORMANCE,pageRequest);
        return setPageResultBean(hits,result);
    }

    /**
     * 分也返回性能，交易，告警数据
     * @param hits
     * @param result
     * @return
     */
    private PageResultBean<List<JSONObject>> setPageResultBean(SearchHits hits, List<JSONObject> result) {
        PageResultBean<List<JSONObject>> pageResultBean = new PageResultBean<>();
        long totalHits = hits.getTotalHits();


        pageResultBean.setTotalNum(totalHits);
        pageResultBean.setResult(result);
        return pageResultBean;
    }
    /**
     * 分也返回性能，交易，告警数据
     * @param hits
     * @param result
     * @return
     */
    private PageResultBean<List<JSONObject>> setPageResultOneToOneBean(SearchHits hits, List<JSONObject> result,PageRequest<?> pageRequest) {
        PageResultBean<List<JSONObject>> pageResultBean = new PageResultBean<>();

        int resultSize=result.size();
        PageEntity page=null;
        if(resultSize<10) {
            page = new PageEntity(resultSize, pageRequest.getPageSize());
        }else{
            page = new PageEntity(pageRequest.getPageNo(), resultSize, pageRequest.getPageSize());
        }
        //获取分页显示的集合
        List<JSONObject> resultJsonList= PageEntity.getResultJsonList(page.getStartRow(),page.getEndRow(),result);
        pageResultBean.setTotalNum(new Long(resultSize));
        pageResultBean.setResult(resultJsonList);
        return pageResultBean;
    }

    /**
     * 根据传入的类型区分是性能，交易，告警表，返回表明
     *
     * @param dataType 查看CommonConstants类
     * @return es索引库的表名字
     */
    private String getEsIndex(int dataType) {
        String indexName = null;
        //性能表
        if (dataType == CommonConstants.PERFORMANCE) {
            indexName = CommonConstants.EsPerfIndex;
        } else if (dataType == CommonConstants.AlERT) {
            indexName = CommonConstants.EsAlertIndex;
        } else if (dataType == CommonConstants.DEAL) {
            indexName = CommonConstants.EsDealIndex;
        }
        return indexName;
    }

    public static void main(String[] args) throws Exception {
        EsUtils esUtils = new EsUtils();
        esUtils.checkinitClient("trustfar-elastic", "172.16.100.205");
        String systimeTime = TimeUtils.getSystimeTime();
        System.out.println(systimeTime);

//        esUtils.deleteIndex("alert_index");
//       esUtils.createIndex("perf_index");
//        testInsert();
        List<Parameter> parameters = new ArrayList<>();
        //1.创建两个参数对象
        Parameter parameter1 = new Parameter("CI_CODE", CommonConstants.OR);
        Parameter parameter2 = new Parameter("MONITOR_TIME", CommonConstants.RANGE);
        Parameter parameter3 = new Parameter("KPI_ID", CommonConstants.SINGLE);
        //2.往第一个参数对象放值
        List<Object> list = new ArrayList<>();
//        list.add("55.49.0.8_TABS_NGCBSIDX104");
//        list.add("7");
//        list.add("8");
        list.add("55.3.16.8_REDHAT_FILE_/");
        list.add("告警描述24052");
        parameter1.setValue(list);
        //3.往第二个参数对象放值
        List<Object> list2 = new ArrayList<>();
        list2.add("20200201055135");
        list2.add("20200509055135");
        parameter2.setValue(list2);
        //4.
        List<Object> list3 = new ArrayList<>();
        list3.add("DB2^tabs_used");
        parameter3.setValue(list3);
        //
        parameters.add(parameter1);
        parameters.add(parameter2);
//        parameters.add(parameter3);
        Map<String, String> ciDataHdfsFile = esUtils.getCiDataHdfsFile(parameters, 2, 1);
        for (Map.Entry<String, String> entry : ciDataHdfsFile.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
        String systimeTime2 = TimeUtils.getSystimeTime();
        System.out.println(systimeTime2);
        esUtils.close();
    }

    /**
     * 关闭es连接
     */
    public void close() {
        if (client != null) {
            client.close();
        }
    }


    /**
     * 设置返回条数为最大值，es默认查询只返回10条
     *
     * @param esIp
     */
    public static void setReturnDataByMaxNumber(String esIp) {
        JSONObject index = new JSONObject();
        JSONObject max_result_window = new JSONObject();
        max_result_window.put("max_result_window", "2147483647");
        index.put("index", max_result_window);
        String url = "http://172.16.100.205:9200/_all/_settings";
        String s = url.replaceAll("172.16.100.205", esIp);
        try {
            String put = HttpClientUtil.put(s, index.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异常定位数据转换为json
     *
     * @param parameters
     * @param dataType
     * @return
     */
    public List<JSONObject> getPositioningJsonDatas(List<Parameter> parameters,
                                                    int dataType) throws ParseException {
        SearchHit[] searchHits = searchHits(parameters, dataType);
        List<JSONObject> resultList = new ArrayList<>();//最终返回的数据
        for (SearchHit documentFields : searchHits) {
            JSONObject jsonObject = new JSONObject();
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            String ci_name = String.valueOf(sourceAsMap.get("CI_NAME"));
            String kpi_name = String.valueOf(sourceAsMap.get("KPI_NAME"));
            String ip = String.valueOf(sourceAsMap.get("DEV_CODE"));
            //格式年月日时分秒20190123100908
            String time = String.valueOf(sourceAsMap.get("MONITOR_TIME"));
            //转换成分钟格式年月日时分2019-01-23 10:09:08
            String timeString = TimeUtils.getStringTimeByStringTime2(time);
            String value = String.valueOf(sourceAsMap.get("VALUE"));
            jsonObject.put("ci_name", ci_name);
            jsonObject.put("kpi_name", kpi_name);
            jsonObject.put("time", timeString);
            jsonObject.put("ip", ip);
            if (value.equalsIgnoreCase("0x0000") || value.equalsIgnoreCase("0x0800")) {
                jsonObject.put("value", 1);
            } else {
                jsonObject.put("value", value);
            }
            resultList.add(jsonObject);
        }
        return resultList;
    }

    /**
     * 获取异常定位数据
     * 需要修改
     * @param parameters
     * @param dataType
     * @return
     * @throws ParseException
     */
    private List<String> getPositioningDatas(List<Parameter> parameters,
                                             int dataType) throws ParseException {
        SearchHit[] searchHits = searchHits(parameters, dataType);
        List<String> resultList = new ArrayList<>();//最终返回的数据
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            String ci_id = String.valueOf(sourceAsMap.get("CI_ID"));
            String kpi_id = String.valueOf(sourceAsMap.get("KPI_ID"));
            //格式年月日时分秒20190123100908
            String time = String.valueOf(sourceAsMap.get("MONITOR_TIME"));
            //转换成分钟格式年月日时分2019-01-23 10:09:08
            String timeString = TimeUtils.getStringTimeByStringTime2(time);
            String value = String.valueOf(sourceAsMap.get("VALUE"));
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ci_id).append(",").append(kpi_id).append(",");
            if (value.equalsIgnoreCase("0x0000") || value.equalsIgnoreCase("0x0800")) {
                stringBuilder.append(timeString).append(",1");
            } else {
                stringBuilder.append(timeString).append(",").append(value);
            }
            resultList.add(stringBuilder.toString());
        }


        return resultList;
    }

    /**
     * 获取日志数据写入hdfs中
     * @param es_index
     * @param cycle
     * @return
     */
    public Boolean putLogData2Hdfs (String es_index, int cycle,String hdfsPath){
        HadoopUtils hadoopUtils = new HadoopUtils();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        Date now = calendar.getTime();
        String nowformat = simpleDateFormat.format(now);
        //这个60可设置为接受参数
        calendar.add(Calendar.MINUTE,-cycle);
        Date time = calendar.getTime();
        String formatbefore = simpleDateFormat.format(time);

        //rangeQurey 第一个参数为字段名，后面是范围 在设置日期格式
        QueryBuilder builder = QueryBuilders.rangeQuery("@timestamp").from(formatbefore).to(nowformat).format("yyyy-MM-dd HH:mm:ss");
        QueryBuilder builder1 = QueryBuilders.rangeQuery("message");
        SearchResponse response = client.prepareSearch(es_index).setQuery(builder).setQuery(builder1)
                .setSize(2147483647)//Elasticsearch支持的最大值是2^31-1
                .get();
        StringBuffer sb = new StringBuffer();
        SearchHits hits = response.getHits();
        JSONObject obj = JSON.parseObject(response.toString());
        List<String> list = new ArrayList();
        try {
            List<Map> hitss = (List<Map>) PropertyUtils.getNestedProperty(obj, "hits.hits");
            if(hits != null){
                for(Map json : hitss){
                    Map _sc = (Map) json.get("_source");
                    //span.put("@timestamp", _sc.get("@timestamp"));
                    //span.put("message", _sc.get("message"));
                    list.add(String.valueOf(_sc.get("message")));
                }
                hadoopUtils.writeByList(hdfsPath,list,"message");
                hadoopUtils.destory();
                return true;
            }
        } catch (Exception  e) {
            e.printStackTrace();
        }

        return false;
    }
}
