package cn.trustfar.aiops.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class PageRequest<T> implements Serializable {
    private T request;
    private Integer pageSize;
    private Integer pageNo;
}
