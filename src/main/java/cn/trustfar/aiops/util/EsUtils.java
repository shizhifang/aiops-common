package cn.trustfar.aiops.util;

import cn.trustfar.aiops.constant.CommonConstants;
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
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lyh
 * Es的连接工具和查询
 */
public class EsUtils {
    private static TransportClient client = null;
    private String indexName = null;
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
     * @param list
     * @param type 1,2,3分别为性能、告警、交易
     * @return
     */
    private static SearchHit[] searchHits(List<Map<String, List<Object>>> list, int type) {
        //1.创建多条件过滤查询器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (Map<String, List<Object>> stringMap : list) {
            for (Map.Entry<String, List<Object>> entry : stringMap.entrySet()) {
                //2.创建具体每个条件的单值过滤器
                List<Object> value = entry.getValue();
                if (value.size() == 2) {
                    RangeQueryBuilder builder = QueryBuilders.rangeQuery(entry.getKey()).gte(value.get(0)).lte(value.get(1));
                    boolQueryBuilder.must(builder);
                } else if (value.size() == 1) {
                    TermQueryBuilder builder = QueryBuilders.termQuery(entry.getKey(), value.get(0));
                    //3.每一个条件的过滤加入到多条件查询中
                    boolQueryBuilder.must(builder);
                }
            }
        }
        //4.查询解析不同的es表，性能表，交易表，告警表
        String typeName = null;
        //性能表
        if (type == CommonConstants.PERFORMANCE) {
            typeName = "perform";
        } else if (type == CommonConstants.AlERT) {
            typeName = "alert";
        } else if (type == CommonConstants.DEAL) {
            typeName = "deal";
        }
        SearchResponse searchResponse = client.prepareSearch("pref_index")
                .setTypes(typeName)
                .setQuery(boolQueryBuilder)
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        return hits1;
    }

    public static String searchByFieldsAndRangeValue(List<Map<String, List<Object>>> list, int type, String path) {
        SearchHit[] searchHits = searchHits(list, type);
        for (SearchHit documentFields : searchHits) {
            System.out.println(documentFields.getSourceAsString());
        }
        return path;
    }

    /**
     * 根据es中的字段的值或者es中的字段的值得范围进行组合过滤查询
     *
     * @param list 字段和值组成的list集合
     *             <Map<String, List<Object>> key值为es中字段，value为es中字段的值
     *             List<Object> 集合有一个值代表key=value，有两个值代表index0<=key<=index1
     * @param type 1,2,3分别为性能、告警、交易
     * @return 返回TIME和VALUE的按, 分割的集合
     */
    public static List<String> searchByFieldsAndRangeValue(List<Map<String, List<Object>>> list, int type) {
        SearchHit[] searchHits = searchHits(list, type);
        List<String> result = new ArrayList<String>();
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            Object time = sourceAsMap.get("MONITOR_TIME");
            System.out.println(time.toString());
            Object value = sourceAsMap.get("VALUE");
            System.out.println(value.toString());
            result.add(time.toString() + "," + value.toString());

//            System.out.println(documentFields.getSourceAsString());
        }
        return result;
    }

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


    public static void main(String[] args) throws UnknownHostException {
        EsUtils.checkinitClient("trustfar-elastic", "172.16.100.204");
//        EsUtils.testInsert();
        List<Map<String, List<Object>>> result = new ArrayList<>();
        Map<String, List<Object>> map = new HashMap<>();
        List<Object> list = new ArrayList<>();
        list.add("ccccccccccc");
        map.put("KPI", list);
        result.add(map);

        Map<String, List<Object>> map2 = new HashMap<>();
        List<Object> list2 = new ArrayList<>();
        list2.add("201902141253");
        map2.put("TIME", list2);
        result.add(map2);

        EsUtils.searchByFieldsAndRangeValue(result, 1);
    }


    public static void close() {
        if (client != null) {
            client.close();
        }
    }
}
