package cn.trustfar.aiops.util;

import cn.trustfar.aiops.constant.CommonConstants;
import cn.trustfar.aiops.pojo.PerfAlertDealData;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PerfAlertDealUtils {



    /**
     * 根据传入的类型区分是性能，交易，告警表，返回表明
     * @param dataType 查看CommonConstants类
     * @return es索引库的表名字
     */
    public static String getIndexType(int dataType) {
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

    public static void createQueryBuilderByPerfAlertDeal(PerfAlertDealData perfAlertDealData, BoolQueryBuilder boolQueryBuilder) {
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
}
