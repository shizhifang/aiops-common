package cn.trustfar.aiops.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * 通用分页模板
 * @param <T>
 */
@Data
public class PageResultBean<T> implements Serializable {
    private T result;
   private Long totalNum;
}
