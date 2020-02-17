package cn.trustfar.aiops.pojo;

import lombok.Data;

@Data
public class PerfAlertDealData {
    private String monitor;
    private String system;
    private String module;
    private String timeStart;
    private String timeEnd;
    private String kpiId;
    private String kpiCode;
    private String kpiName;
    private String kpiShortName;
    private String CI_ID_LIST;
}
