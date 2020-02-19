package cn.trustfar.aiops.util;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TimeUtils {
    public static Calendar getMinuteTimeByStringMinuteTime(String time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        Date paramDate=null;
        Calendar calendar = Calendar.getInstance();
        try {
            paramDate = sdf.parse(time);
            calendar.setTime(paramDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return calendar;
    }
    public static String getMinuteTimeByMinuteCalendar(Calendar calendar){
        Date time = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(time);

    }
    public static String getSystimeTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

    public static void main(String[] args) {
//        Calendar time1 = TimeUtils.getMinuteTimeByStringTime("201901231009");
//        Calendar time2 = TimeUtils.getMinuteTimeByStringTime("201901231019");
//        String minuteTimeByCalendar = TimeUtils.getMinuteTimeByCalendar(time1);
//        System.out.println(minuteTimeByCalendar);
        String firstTime = "201901231009";
        Calendar firstCalendar = TimeUtils.getMinuteTimeByStringMinuteTime(firstTime);
        String endTime = "201901231059";
        Calendar endCalendar = TimeUtils.getMinuteTimeByStringMinuteTime(endTime);
        long tmp = (endCalendar.getTime().getTime() - firstCalendar.getTime().getTime()) / (60 * 10000);
        Calendar nextCalendar=null;
        for(int i=0;i<tmp;i++){
            firstCalendar.add(Calendar.MINUTE, 5);
            nextCalendar=firstCalendar;
            System.out.println(TimeUtils.getMinuteTimeByMinuteCalendar(nextCalendar));
        }
//        timestampByDate.add(Calendar.MINUTE,5);
//        Date time = timestampByDate.getTime();
//        System.out.println(time.toString());
        System.out.println(TimeUtils.getSystimeTime());
        String a="20190123100908";
        String substring = a.substring(0, a.length() - 2);
        System.out.println(substring);
        List<String> list=new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("b");
        list.add("c");
        int i = list.indexOf("b");
        System.out.println(i);
    }

}
