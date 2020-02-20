package cn.trustfar.aiops.util;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TimeUtils {
    public static Calendar getCalendarByStringMinuteTime(String time){
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

    public static String getStringByMinuteCalendar(Calendar calendar){
        Date time = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(time);

    }
    public static String getSystimeTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

    public static void main(String[] args) {
        String firstTime = "201901231009";
        Calendar firstCalendar = TimeUtils.getCalendarByStringMinuteTime(firstTime);
        String endTime = "201901231010";
        Calendar endCalendar = TimeUtils.getCalendarByStringMinuteTime(endTime);
        System.out.println(firstCalendar.getTime().getTime()/ (60 * 10000));
        System.out.println(endCalendar.getTime().getTime()/ (60 * 10000));
        long tmp = (endCalendar.getTime().getTime() - firstCalendar.getTime().getTime()) / (60 * 10000);
    }

}
