package cn.trustfar.aiops.pojo;


import java.io.Serializable;
import java.util.List;

@lombok.Data
public class Parameter implements Serializable{
    //要过滤的名字
    private String name;
    //过滤名字的值,key有多个，多个需要有确定值之间的关系
    private List<Object> value;
    //值之间的关系,目前三种情况
    // CommonConstants.OR 或的情况         value可以有多个值
    // CommonConstants.RANGE 范围的情况    value只能有2个值
    // CommonConstants.SINGLE              value一个值
    private String valueType;

    public Parameter(String name, String valueType) {
        this.name = name;
        this.valueType = valueType;
    }

    public Parameter() {
    }
}
