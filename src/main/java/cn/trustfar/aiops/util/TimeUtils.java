package cn.trustfar.aiops.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;

public class TimeUtils {
    static SimpleDateFormat sdfSecond1 = new SimpleDateFormat("yyyyMMddHHmmss");
    static SimpleDateFormat sdfSecond2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static DateTimeFormatter getSdfSecond1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    static DateTimeFormatter getSdfSecond2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static DateTimeFormatter getSdfSecond3 = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");
    private static Logger logger = LoggerFactory.getLogger(TimeUtils.class.getSimpleName());

    /**
     * 把格式为yyyyMMddHHmmss的时间字符串转换成yyyy-MM-dd HH:mm:ss的时间字符串
     * 并且处理掉秒
     *
     * @param time 20190123100923
     * @return 2019-01-23 10:09:00
     */
    public static String getStringTimeByStringTime(String time) {
        String format = null;
        try {
            LocalDateTime parse = LocalDateTime.parse(time, getSdfSecond1);
            LocalDateTime localDateTime = parse.withSecond(0);
            format = localDateTime.format(getSdfSecond2);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("这个时间------{}------转换错误", time);
        }
        return format;
    }

    /**
     * 把格式为yyyy-MM-dd HH:mm:ss的时间字符串转换成yyyyMMddHHmmss的时间字符串
     *
     * @param time 2019-01-23 10:09:23
     * @return 20190123100923
     */
    public static String getStringTimeByStringTime3(String time) throws Exception {
        String format = null;
        try {
            LocalDateTime parse = LocalDateTime.parse(time, getSdfSecond2);
            format = parse.format(getSdfSecond1);
        } catch (Exception e) {
            throw new Exception(e.toString());
        }
        return format;
    }

    /**
     * 把格式为yyyyMMddHHmmss的时间字符串转换成yyyy-MM-dd HH:mm:ss的时间字符串
     *
     * @param time 20190123100923
     * @return 2019-01-23 10:09:23
     */
    public static String getStringTimeByStringTime2(String time) throws ParseException {
        String format = null;
        try {
            LocalDateTime parse = LocalDateTime.parse(time, getSdfSecond1);
            format = parse.format(getSdfSecond2);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("这个时间------{}------转换错误", time);
        }
        return format;
    }

    /**
     * 获取系统时间格式yyyyMMdd_HHmmss
     *
     * @return
     */
    public static String getSystimeTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        return sdf.format(new Date());
    }

    /**
     * 获取系统时间格式yyyyMMdd_HHmmssSSS
     *
     * @return
     */
    public static String getSystimeMsTime() {
        return getSdfSecond3.format(LocalDateTime.now());
    }
//    /**
//     * 把格式为yyyyMMddHHmmss的时间字符串转换成yyyy-MM-dd HH:mm:ss的时间字符串
//     * 并且处理掉秒
//     * @param time 20190123100923
//     * @return 2019-01-23 10:09:00
//     */
//    public static String getStringTimeByStringTime(String time) throws ParseException {
//        Date paramDate = sdfSecond1.parse(time);
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTime(paramDate);
//        calendar.set(Calendar.SECOND,0);
//        String format = sdfSecond2.format(calendar.getTime());
//        return format;
//    }
//    /**
//     * 把格式为yyyyMMddHHmmss的时间字符串转换成yyyy-MM-dd HH:mm:ss的时间字符串
//     * @param time 20190123100923
//     * @return 2019-01-23 10:09:23
//     */
//    public static String getStringTImeByStringTime2(String time) throws ParseException {
//        Date paramDate = sdfSecond1.parse(time);
//        String format = sdfSecond2.format(paramDate.getTime());
//        return format;
//    }


    public static void main(String[] args) throws Exception {
        String systimeMsTime = TimeUtils.getSystimeMsTime();
//        System.out.println(systimeMsTime);
        String firstTime = "20190123100923";
        String endTime = "20190123101000";
        String geta = getStringTimeByStringTime2(firstTime);
        String stringTimeByStringTime3 = getStringTimeByStringTime3("2020-03-04 20:40:22");
        System.out.println(stringTimeByStringTime3);
        System.out.println(geta);
        String timeString = TimeUtils.getStringTimeByStringTime("343");
        ArrayList<String> resultList = new ArrayList<>();
        if (StringUtils.isNotEmpty(timeString)) {
            resultList.add(timeString + "," + "1");
            System.out.println(resultList.get(0));
        }

    }

}
