package cn.trustfar.aiops.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class PerfAlertDealData implements Serializable {
    private String monitor;
    private String system;
    private String module;
    private String timeStart;
    private String timeEnd;
    private String kpiId;
    private String kpiCode;
    private String kpiName;
    private String kpiShortName;
    private String alertLevel;
    private List<String> CI_ID_LIST;
}
