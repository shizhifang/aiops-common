package cn.trustfar.aiops.pojo;

import lombok.Data;

@Data
public class Ci {
    private String ciTypeId;
    private String ciTypeCode;
    private String ciTypeName;
    private String parentCiTypeId;
    private String parentCiId;
}
