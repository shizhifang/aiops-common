package cn.trustfar.aiops.util;

import cn.trustfar.aiops.bean.AlertResultBean;
import cn.trustfar.aiops.bean.CommonReustBean;
import cn.trustfar.aiops.bean.DealReulstBean;
import cn.trustfar.aiops.bean.PerfReusltBean;
import cn.trustfar.aiops.constant.CommonConstants;
import cn.trustfar.aiops.pojo.Parameter;
import cn.trustfar.aiops.pojo.PerfAlertDealData;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * @author lyh
 * Es的连接工具和查询
 */
public class EsUtils {
    private static TransportClient client = null;
    /**
     * 初始化es的客户端连接工具
     *
     * @param esName es集群的名字trustfar-elastic
     * @param esIp   es的ip地址"192.168.10.104"
     * @return
     */
    public static TransportClient checkinitClient(String esName, String esIp) {
        if (client == null) {
            try {
                Settings settings = Settings.builder()
                        .put("cluster.name", esName)
                        //开启嗅探机制，自动发现es集群的服务器
                        .put("client.transport.sniff", "true")
                        .build();
                client = new PreBuiltTransportClient(settings)
                        .addTransportAddresses(new TransportAddress(InetAddress.getByName(esIp), 9300));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return client;
    }

    /**
     * 创建es的索引库
     *
     * @param indexName
     */
    public static void createIndex(String indexName) {
        client.admin().indices().prepareCreate(indexName).get();
    }

    /**
     * 删除索引
     *
     * @param indexName
     */
    public static void deleteIndex(String indexName) {
        client.admin().indices().prepareDelete(indexName).get();
    }

    /**
     * 批量把json数据插入es
     *6.4版本index下面只能有一个type
     * @param jsonDatas
     * @param indexName es的索引库名必须是小写英文字母不能以下划线开头
     * @return
     * @throws UnknownHostException
     */
    public static boolean batchInsert(List<String> jsonDatas, String indexName) throws UnknownHostException {
        System.out.println("将要插入es的数据量-----------》" + jsonDatas.size());
        if (jsonDatas.size() == 0) {
            System.out.println("此批次无es数据插入");
            return false;
        }
        BulkRequestBuilder bulk = client.prepareBulk();
        for (String jsonData : jsonDatas) {
            bulk.add(buildIndex(jsonData, indexName, CommonConstants.EsIndexType));
        }
        BulkResponse bulkItemResponses = bulk.execute().actionGet();
        RestStatus status = bulkItemResponses.status();
        System.out.println(status);
        return "OK".equals(status.toString());
    }

    /**
     * es数据库添加一条json数据
     *6.4版本index下面只能有一个type
     * @param jsonData  要添加的数据
     * @param indexName es索引库名
     * @param typeName  es索引库表名
     * @return
     */
    public static IndexRequestBuilder buildIndex(String jsonData, String indexName, String typeName) {
        return client.prepareIndex(indexName, typeName).setSource(jsonData, XContentType.JSON);
    }


    /**
     * @param list     具体查看Parameter
     * @param dataType 1,2,3分别为性能、告警、交易
     * @return
     */
    private static SearchHit[] searchHits(List<Parameter> list, int dataType) {
        //1.创建多条件过滤查询器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //2.
        for (Parameter parameter : list) {
            String name = parameter.getName();
            List<Object> value = parameter.getValue();
            String valueType = parameter.getValueType();
            //3.根据值得关系创建不同过滤器
            if (valueType.equalsIgnoreCase(CommonConstants.OR)) {
                if (value.size() == 1) {
                    TermQueryBuilder builder = QueryBuilders.termQuery(name, value.get(0));
                    //每一个条件的过滤加入到多条件查询中
                    boolQueryBuilder.must(builder);
                } else {
                    QueryBuilders.termsQuery(name, value);
                }
            } else if (valueType.equalsIgnoreCase(CommonConstants.RANGE)) {
                RangeQueryBuilder builder = QueryBuilders.rangeQuery(name).gte(value.get(0)).lte(value.get(1));
                //每一个条件的过滤加入到多条件查询中
                boolQueryBuilder.must(builder);
            } else if (valueType.equalsIgnoreCase(CommonConstants.SINGLE)) {
                TermQueryBuilder builder = QueryBuilders.termQuery(name, value.get(0));
                boolQueryBuilder.must(builder);
            }
        }
        //4.查询解析不同的es表，性能表，交易表，告警表
        String esIndex = getEsIndex(dataType);
        //按照时间字段排序
        FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort("MONITOR_TIME");
        //5.client访问es返回hits
        SearchResponse searchResponse = client.prepareSearch(esIndex)
                .setTypes(CommonConstants.EsIndexType)
                .setQuery(boolQueryBuilder)
//                .addSort(fieldSortBuilder)
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        return hits1;
    }



    /**
     *
     * @param list 查看Parameter类
     * @param dataType CommonConstants.PERFORMANCE.AlERT.DEAL
     *@param sendFrequency 数据发送的频率,用于缺失值计算
     * @return
     */
    public static List<String> getListTimeAndValue(List<Parameter> list,
                                                       int dataType,
                                                       int sendFrequency){
        SearchHit[] searchHits = searchHits(list, dataType);
        List<String> resultList = new ArrayList<>();//最终返回的数据
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            //格式年月日时分秒20190123100908
            Object time = sourceAsMap.get("MONITOR_TIME");
            //转换成分钟格式年月日时分20190123100900
            String minuteTime = time.toString().substring(0, time.toString().length() - 2)+"00";
            Object value = sourceAsMap.get("VALUE");
            resultList.add(minuteTime+","+value.toString());
        }
        //TODO缺失值计算
        int missFrequency = sendFrequency * CommonConstants.Unit_five;
        return resultList;
    }

    /**
     * 测试插入代码
     * @throws UnknownHostException
     */
    public static void testInsert() throws UnknownHostException {
        List<String> jsonDatas = new ArrayList<>();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("VALUE", "1251");
        jsonObject.put("KPI", "ccccccccccc");
        jsonObject.put("CI", "xxxxxxxxx");
        jsonObject.put("TIME", "201902141253");
        jsonDatas.add(jsonObject.toString());

        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("VALUE", "1252");
        jsonObject1.put("KPI", "ddddddddddd");
        jsonObject1.put("CI", "ppppppppppp");
        jsonObject1.put("TIME", "201902141252");
//        jsonDatas.add(jsonObject1.toString());
        EsUtils.batchInsert(jsonDatas, "pref_index");
    }

    /**
     *
     * @param perfAlertDealData
     * @param dataType
     * @return
     */
    private static SearchHit[] searchByPerfAlertDealData(PerfAlertDealData perfAlertDealData, int dataType) {
        //1.创建多条件过滤查询器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //2.获取要过滤的字段放入map中 key值为过滤的字段名字，value为过滤字段名字的值
        String ci_id_list = perfAlertDealData.getCI_ID_LIST();
        String timeStart = perfAlertDealData.getTimeStart();
        String timeEnd = perfAlertDealData.getTimeEnd();
        String monitor = perfAlertDealData.getMonitor();
        String module = perfAlertDealData.getModule();
        String system = perfAlertDealData.getSystem();
        String kpiCode = perfAlertDealData.getKpiCode();
        String kpiId = perfAlertDealData.getKpiId();
        String kpiName = perfAlertDealData.getKpiName();
        String kpiShortName = perfAlertDealData.getKpiShortName();
        Map<String, String> map = new HashMap<>();
        map.put("MONITOR", monitor);
        map.put("SYSTEM", system);
        map.put("MODULE", module);
        map.put("KPI_ID", kpiId);
        map.put("KPI_CODE", kpiCode);
        map.put("KPI_NAME", kpiName);
        map.put("KPI_SHORT_NAME", kpiShortName);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isNotBlank(value)) {
                TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(key, value);
                boolQueryBuilder.must(termQueryBuilder);
            }
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
        //4.查询解析不同的es表，性能表，交易表，告警表
        String indexName = getEsIndex(dataType);
        //5.client访问es返回hits
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(CommonConstants.EsIndexType)
                .setQuery(boolQueryBuilder)
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        return hits1;
    }

    /**
     * 根据查询到的数据，获取性能、交易、告警接口根据条件查询需要返回的字段和值并放入json中
     * @param searchHits es查询后返回的所有数据
     * @param dataType 查看CommonConstants类
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
                //TODO
                jsonObject.put(perfReusltBean.KPI_UNIT, sourceAsMap.get(perfReusltBean.KPI_UNIT));
                jsonObject.put(perfReusltBean.KPI_VAL_TYPE, sourceAsMap.get(perfReusltBean.KPI_VAL_TYPE));
                resultList.add(jsonObject);
            }

        } else if (dataType == CommonConstants.AlERT) {
            AlertResultBean alertResultBean = new AlertResultBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                JSONObject jsonObject = new JSONObject();
                addCommonReustBean2JsonList(sourceAsMap, jsonObject);
                //TODO
                resultList.add(jsonObject);
            }
        } else if (dataType == CommonConstants.DEAL) {
            DealReulstBean dealReulstBean = new DealReulstBean();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                JSONObject jsonObject = new JSONObject();
                addCommonReustBean2JsonList(sourceAsMap, jsonObject);
                //TODO
                jsonObject.put(dealReulstBean.KPI_UNIT, sourceAsMap.get(dealReulstBean.KPI_UNIT));
                jsonObject.put(dealReulstBean.KPI_VAL_TYPE, sourceAsMap.get(dealReulstBean.KPI_VAL_TYPE));
                resultList.add(jsonObject);
            }
        }
        return resultList;
    }

    /**
     * 性能，交易，告警共用的字段放到返回的JSONObject中
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
     * 离线数据接口，根据提供的条件查询性能，交易数据
     * @param perfAlertDealData
     * @param dataType 查看CommonConstants类
     * @return
     */
    public static List<JSONObject> searchPerfOrDeal(PerfAlertDealData perfAlertDealData, int dataType) {
        SearchHit[] searchHits = searchByPerfAlertDealData(perfAlertDealData, dataType);
        List<JSONObject> result = getJsonListByDataType(searchHits, dataType);
        return result;
    }

    /**
     * 离线数据接口，根据提供的条件查询alert数据
     * @param perfAlertDealData
     * @param dataType 查看CommonConstants类
     * @return
     */
    public static List<JSONObject> searchAlert(PerfAlertDealData perfAlertDealData, int dataType) {
        SearchHit[] searchHits = searchByPerfAlertDealData(perfAlertDealData, dataType);
        List<JSONObject> result = getJsonListByDataType(searchHits, dataType);
        return result;
    }

    /**
     * 根据传入的类型区分是性能，交易，告警表，返回表明
     *
     * @param dataType 查看CommonConstants类
     * @return es索引库的表名字
     */
    private static String getEsIndex(int dataType) {
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
        EsUtils.checkinitClient("trustfar-elastic","172.16.100.205");
//        deleteIndex("pref_index");
//        createIndex("pref_index");
//        testInsert();
        List<Parameter> parameters=new ArrayList<>();
        //1.创建两个参数对象
        Parameter parameter1 = new Parameter("CI_ID",CommonConstants.SINGLE);
        Parameter parameter2 = new Parameter("MONITOR_TIME",CommonConstants.RANGE);
        //2.往第一个参数对象放值
        List<Object> list = new ArrayList<>();
        list.add("26");
        parameter1.setValue(list);
        //3.往第二个参数对象放值
        List<Object> list2 = new ArrayList<>();
        list2.add("20200108000100");
        list2.add("20201108000100");
        parameter2.setValue(list2);
        parameters.add(parameter1);
        parameters.add(parameter2);
        List<String> listTimeAndValue = EsUtils.getListTimeAndValue(parameters, 1, 1);
        HadoopUtils.connHadoopByHA();
        HadoopUtils.writeByList("/tmp/aaa.txt",listTimeAndValue);
        for (String s : listTimeAndValue) {
            System.out.println(s);
        }
        EsUtils.close();
    }

    /**
     * 关闭es连接
     */
    public static void close() {
        if (client != null) {
            client.close();
        }
    }


}
