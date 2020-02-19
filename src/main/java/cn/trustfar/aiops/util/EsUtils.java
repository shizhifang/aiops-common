package cn.trustfar.aiops.util;

import cn.trustfar.aiops.bean.AlertResultBean;
import cn.trustfar.aiops.bean.CommonReustBean;
import cn.trustfar.aiops.bean.DealReulstBean;
import cn.trustfar.aiops.bean.PerfReusltBean;
import cn.trustfar.aiops.constant.CommonConstants;
import cn.trustfar.aiops.pojo.Parameter;
import cn.trustfar.aiops.pojo.PerfAlertDealData;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
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
    private static String indexName = "pref_index";
    private String typeNmae = null;

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
     *
     * @param jsonDatas
     * @param indexName es的索引库名必须是小写英文字母不能以下划线开头
     * @param typeName  es索引库表名
     * @return
     * @throws UnknownHostException
     */
    public static boolean batchInsert(List<String> jsonDatas, String indexName, String typeName) throws UnknownHostException {
        System.out.println("插入es的数据量-----------》" + jsonDatas.size());
        if (jsonDatas.size() == 0) {
            return false;
        }
        BulkRequestBuilder bulk = client.prepareBulk();
        for (String jsonData : jsonDatas) {
            bulk.add(buildIndex(jsonData, indexName, typeName));
        }
        BulkResponse bulkItemResponses = bulk.execute().actionGet();
        RestStatus status = bulkItemResponses.status();
        System.out.println(status);
        return "OK".equals(status.toString());
    }

    /**
     * es数据库添加一条json数据
     *
     * @param jsonData  要添加的数据
     * @param indexName es索引库名
     * @param typeName  es索引库表名
     * @return
     */
    public static IndexRequestBuilder buildIndex(String jsonData, String indexName, String typeName) {
        return client.prepareIndex(indexName, typeName).setSource(jsonData, XContentType.JSON);
    }

    /**
     * 查询索引库中某个表的所有数据
     *
     * @param indexName 索引库名
     * @param typeName  表名
     *                  example：结果为一条条json串
     *                  {"KPI_UNIT":"KPI_UNIT_16","KPI":"KPI_16","CI_ID":"CI_IDssssssa7","CI_CODE":"123612367216","VALUE":"VALUE_16","DEV_CODE":"192.168.0.17","KPI_ID":"KPI_ID_7","CI_TYPE_NAME":"交换机","MONITOR_TIME":"20200120151623","CI_NAME":"CI_NAME_1","CI_TYPE_ID":"在线字数统计16","KPI_NAME":"KPI_NAME_16","KPI_CODE":"KPI_CODE_16","rowKey":"11921680017002020012015KPI_ID_7CI_IDssssssa7"}
     *                  {"KPI_UNIT":"KPI_UNIT_19","KPI":"KPI_19","CI_ID":"CI_IDssssssa1","CI_CODE":"123612367219","VALUE":"VALUE_19","DEV_CODE":"192.168.0.11","KPI_ID":"KPI_ID_1","CI_TYPE_NAME":"交换机","MONITOR_TIME":"20200120151925","CI_NAME":"CI_NAME_1","CI_TYPE_ID":"在线字数统计19","KPI_NAME":"KPI_NAME_19","KPI_CODE":"KPI_CODE_19","rowKey":"11921680011002020012015KPI_ID_1CI_IDssssssa1"}
     */
    public static void queryAll(String indexName, String typeName) {
        SearchResponse searchResponse = client
                .prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(new MatchAllQueryBuilder())
                .get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 查找es中某个字段的范围
     * 列如查找年龄18到28的人,不包含18和28
     *
     * @param indexName es索引库名
     * @param typeName  es表名
     * @param field     字段名
     * @param gtValue   大于某个值
     * @param ltValue   小于某个值
     */
    public static void rangeQuery(String indexName, String typeName, String field, String gtValue, String ltValue) {
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(new RangeQueryBuilder(field).gt(gtValue).lt(ltValue))
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
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
        String typeName = getEsIndexType(dataType);
        //按照时间字段排序
        FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort("TIME");
        //5.client访问es返回hits
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(boolQueryBuilder)
                .addSort(fieldSortBuilder)
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        return hits1;
    }

    /**
     * 根据es中的字段的值或者es中的字段的值得范围进行组合过滤查询，查询结果放在hdfs上面
     *
     * @param list            具体查看Parameter
     * @param dataType        CommonConstants.PERFORMANCE、AlERT、DEAL
     * @param timeType        CommonConstants.MINUTE、HOUR
     * @param timeInterval    时间间隔,表示隔多少时间的数据为一
     * @param dataProcessType CommonConstants.AVG、SUM
     * @param path            hdfs路径
     * @return hdfs路徑
     * @throws Exception
     */
    public static String searchByFieldsAndRangeValue(List<Parameter> list, int dataType, int timeType, int timeInterval, int dataProcessType, String path) throws Exception {
        HadoopUtils.connHadoopByHA();
        SearchHit[] searchHits = searchHits(list, dataType);
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            Object time = sourceAsMap.get("TIME");
            System.out.println(time.toString());
            Object value = sourceAsMap.get("VALUE");
            System.out.println(value.toString());
//            System.out.println(documentFields.getSourceAsString());
        }
        return path;
    }

    /**
     * 根据es中的字段的值或者es中的字段的值得范围进行组合过滤查询
     *
     * @param list            具体查看Parameter
     * @param dataType        CommonConstants.PERFORMANCE、AlERT、DEAL
     * @param timeType        CommonConstants.MINUTE、HOUR
     * @param timeInterval    时间间隔
     * @param dataProcessType CommonConstants.AVG、SUM
     * @return
     */
    public static Map<String,String> searchByFieldsAndRangeValue(List<Parameter> list, int dataType, int timeType, int timeInterval, int dataProcessType) {
        SearchHit[] searchHits = searchHits(list, dataType);
        Map<String,String> resultMap = new LinkedHashMap<>();//最终返回的数据key=时间,
        List<String> timeList = new ArrayList<>();//存储分钟时间从小到大
        List<String> valueList = new ArrayList<>();//存储分钟时间对应的值
        for (int i = 0; i < searchHits.length; i++) {
            Map<String, Object> sourceAsMap = searchHits[i].getSourceAsMap();
        }
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            //格式年月日时分秒20190123100908
            Object time = sourceAsMap.get("TIME");
            //转换成分钟格式年月日时分201901231009
            timeList.add(time.toString().substring(0, time.toString().length() - 2));
            System.out.println(time.toString());
            Object value = sourceAsMap.get("VALUE");
            System.out.println(value.toString());
            valueList.add(value.toString());
//            System.out.println(documentFields.getSourceAsString());
        }
        if (timeType == CommonConstants.MINUTE && timeInterval != 1) {
            //time格式201901231009
            String firstTime = timeList.get(0);
            Calendar firstCalendar = TimeUtils.getMinuteTimeByStringMinuteTime(firstTime);
            String endTime = timeList.get(timeList.size() - 1);
            Calendar endCalendar = TimeUtils.getMinuteTimeByStringMinuteTime(endTime);
            long tmp = (endCalendar.getTime().getTime() - firstCalendar.getTime().getTime()) / (60 * 10000);
            Calendar nextCalendar = null;
//            resultMap.put(firstTime,valueList.get(0));
            for (int i = 0; i < tmp; i++) {
                firstCalendar.add(Calendar.MINUTE, timeInterval);
                //nextCalendar的时间为最终要返回的时间
                nextCalendar = firstCalendar;
                String next = TimeUtils.getMinuteTimeByMinuteCalendar(nextCalendar);
                int indexOf = timeList.indexOf(next);
                String value=null;
                for(int j=0;j<indexOf;j++){
//                    valueList.get(j)
                }
                if (indexOf < 0) {

                }
            }
        }
        return resultMap;
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
        EsUtils.batchInsert(jsonDatas, "pref_index", "prefTable");
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
        String typeName = getEsIndexType(dataType);
        //5.client访问es返回hits
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
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
    private static String getEsIndexType(int dataType) {
        String typeName = null;
        //性能表
        if (dataType == CommonConstants.PERFORMANCE) {
            typeName = "prefTable";
        } else if (dataType == CommonConstants.AlERT) {
            typeName = "alertTable";
        } else if (dataType == CommonConstants.DEAL) {
            typeName = "dealTable";
        }
        return typeName;
    }

    public static void main(String[] args) throws UnknownHostException {
        PerfAlertDealData perfAlertDealData = new PerfAlertDealData();
        perfAlertDealData.setKpiId("sssssss");
        String timeEnd = perfAlertDealData.getTimeEnd();
        boolean empty = StringUtils.isEmpty(timeEnd);
        System.out.println(empty);
        System.out.println(timeEnd);
//        EsUtils.checkinitClient("trustfar-elastic", "172.16.100.204");
////        EsUtils.testInsert();
//        List<Parameter> result = new ArrayList<>();
//        Parameter parameter1 = new Parameter();
//        Parameter parameter2 = new Parameter();
//        parameter1.setName("KPI");
//        List<Object> list = new ArrayList<>();
//        list.add("ccccccccccc");
//        parameter1.setValue(list);
//        parameter1.setValueType(CommonConstants.SINGLE);
//        parameter2.setName("TIME");
//        List<Object> list2 = new ArrayList<>();
//        list2.add("201902141253");
//        parameter2.setValue(list2);
//        parameter2.setValueType(CommonConstants.SINGLE);
//        result.add(parameter1);
//        result.add(parameter2);
//        EsUtils.searchByFieldsAndRangeValue(result, 1, CommonConstants.MINUTE, 5, CommonConstants.AVG);
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
