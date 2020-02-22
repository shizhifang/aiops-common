package cn.trustfar.aiops.util;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TimeUtils {
    static SimpleDateFormat sdfSecond1 = new SimpleDateFormat("yyyyMMddHHmmss");
    static SimpleDateFormat sdfSecond2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     * 把格式为yyyyMMddHHmmss的时间字符串转换成yyyy-MM-dd HH:mm:ss的时间字符串
     * 并且处理掉秒
     * @param time 20190123100923
     * @return 2019-01-23 10:09:00
     */
    public static String getStringTimeByStringTime(String time) throws ParseException {
        Date paramDate = sdfSecond1.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(paramDate);
        calendar.set(Calendar.SECOND,0);
        String format = sdfSecond2.format(calendar.getTime());
        return format;
    }


    public static void main(String[] args) throws ParseException {
        String firstTime = "20190123100923";
        String endTime = "20190123101000";
        for(int i=0;i<100;i++){
            String stringTimeByStringSecondTime = TimeUtils.getStringTimeByStringTime(firstTime);
            System.out.println(stringTimeByStringSecondTime);
        }
    }

}
