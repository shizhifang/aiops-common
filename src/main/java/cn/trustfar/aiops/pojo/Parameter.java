package cn.trustfar.aiops.pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

@lombok.Data
public class Parameter {
    //要过滤的名字
    private String name;
    //过滤名字的值,key有多个，多个需要有确定值之间的关系
    private List<Object> value;
    //值之间的关系,目前有两种情况
    // 或的情况         value可以有多个值
    // 范围的情况       value只能有2个值
    private String valueType;

    public Parameter(String name, String valueType) {
        this.name = name;
        this.valueType = valueType;
    }

    public Parameter() {
    }
}
