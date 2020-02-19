package cn.trustfar.aiops.constant;

import java.util.*;

public class Test {
    public static void main(String[] args) {
        List<String> list=new ArrayList<>();
        list.add("20190801");
        list.add("20210801");
        list.add("20200801");
        System.out.println(list.toString());
        Collections.sort(list);
        System.out.println(list.toString());
    }
}
