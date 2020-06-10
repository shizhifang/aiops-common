package cn.trustfar.aiops.util;

/**
 * @author yangyl
 * @version V0.0.1
 * @date 2019.10.20
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtil {

    private static final ThreadLocal<SimpleDateFormat> threadLocal = new ThreadLocal<SimpleDateFormat>();
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    private static final Logger LOG = LoggerFactory.getLogger(DateUtil.class);
    private static final Object object = new Object();



    public DateUtil() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * 获得两个日期之间的月份
     *
     * @param openDate
     * @param applyTime
     * @return
     * @author rongjianmin
     */
    public static double getIntervalMonths(String openDate, String applyTime) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        if (openDate != null && openDate.contains("/")) {
            openDate = openDate.replace("/", "-");
        }
        if (applyTime != null && applyTime.contains("/")) {
            applyTime = applyTime.replace("/", "-");
        }
        Calendar firstDate = Calendar.getInstance();// 申请时间（后时间）
        Calendar secDate = Calendar.getInstance();// 前时间
        double months = 0;// 相隔多少月
        try {
            if (openDate == null) {
                return 0.0;
            }
            if (applyTime != null || !"".equals(applyTime)) {
                firstDate.setTime(df.parse(applyTime));
            }


            Date opDate = StringToDate(openDate);
            secDate.setTime(opDate);
            long firstDateMill = firstDate.getTimeInMillis();
            long secDateMill = secDate.getTimeInMillis();
            long days = (firstDateMill - secDateMill) / (1000 * 60 * 60 * 24);// 相隔天数
            months = days / 30.0;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return months;
    }

    /**
     * 比较两个日期相差的月份
     *
     * @param date1
     * @param date2
     * @return
     */
    public static int calculateMonthIn(Date date1, Date date2) {
        Calendar cal1 = new GregorianCalendar();
        cal1.setTime(date1);
        Calendar cal2 = new GregorianCalendar();
        cal2.setTime(date2);
        int c =
                (cal1.get(Calendar.YEAR) - cal2.get(Calendar.YEAR)) * 12 + cal1.get(Calendar.MONTH)
                        - cal2.get(Calendar.MONTH);

        return c;
    }

    /**
     * 计算两个日期之间相差的月数
     *
     * @param date1
     * @param date2
     * @return
     */
    public static int getMonths(Date date1, Date date2) {
        int iMonth = 0;
        int flag = 0;
        try {
            Calendar objCalendarDate1 = Calendar.getInstance();
            objCalendarDate1.setTime(date1);

            Calendar objCalendarDate2 = Calendar.getInstance();
            objCalendarDate2.setTime(date2);

            if (objCalendarDate2.equals(objCalendarDate1)) {
                return 0;
            }
            if (objCalendarDate1.after(objCalendarDate2)) {
                Calendar temp = objCalendarDate1;
                objCalendarDate1 = objCalendarDate2;
                objCalendarDate2 = temp;
            }
            if (objCalendarDate2.get(Calendar.DAY_OF_MONTH) < objCalendarDate1.get(Calendar.DAY_OF_MONTH)) {
                flag = 1;
            }
            if (objCalendarDate2.get(Calendar.YEAR) > objCalendarDate1.get(Calendar.YEAR)) {
                iMonth = ((objCalendarDate2.get(Calendar.YEAR) - objCalendarDate1.get(Calendar.YEAR))
                        * 12 + objCalendarDate2.get(Calendar.MONTH) - flag)
                        - objCalendarDate1.get(Calendar.MONTH);
            }else {
                iMonth = objCalendarDate2.get(Calendar.MONTH)
                        - objCalendarDate1.get(Calendar.MONTH) - flag;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return iMonth;
    }

    public static int getMonthSpace(String date1, String date2)
            throws ParseException {

        int result = 0;
        if (date1 != null && date1.contains("/")) {
            date1 = date1.replace("/", "-");
        }
        if (date2 != null && date2.contains("/")) {
            date2 = date2.replace("/", "-");
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();

        c1.setTime(sdf.parse(date1));
        c2.setTime(sdf.parse(date2));

        result = c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH);

        return result == 0 ? 1 : Math.abs(result);

    }

    /**
     * 计算n月（天）前的时间
     *
     * @param applyTime
     *            传入的时间如2015-05-05
     * @param i
     *            传入的月份(天)间隔数 6
     * @param flag
     *            DAY or MONTH
     * @return 返回2014-11-05
     * @author rongjianmin
     */
    public static String getDateFormat(String applyTime, int i, String flag) {
        if (applyTime != null && applyTime.contains("/")) {
            applyTime = applyTime.replace("/", "-");
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();
        String date = null;
        if (applyTime == null) {
            return null;
        }
        if (flag != null && flag.equals("DAY")) {
            try {
                c.setTime(df.parse(applyTime));
                c.set(Calendar.DATE, c.get(Calendar.DATE) - i);
                date = df.format(c.getTime());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (flag != null && flag.equals("MONTH")) {
            try {
                c.setTime(df.parse(applyTime));
                c.set(Calendar.MONTH, c.get(Calendar.MONTH) - i);
                date = df.format(c.getTime());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return date;
    }

    public static String getDateFormat(Date applyTime) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(applyTime);
    }

    public static Date getDateFromStr(String applyTime) throws ParseException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        if (applyTime == null) {
            return null;
        }
        return df.parse(applyTime);
    }

    public static int getTotalDaysOfMonth(String applyTime) throws ParseException {
        return getTotalDaysOfMonth(getDateFromStr(applyTime));
    }

    @SuppressWarnings("static-access")
    public static int getTotalDaysOfMonth(Date applyTime) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(applyTime);
        return calendar.getActualMaximum(calendar.DAY_OF_MONTH);
    }

    /**
     * BigDecimal null to 0
     *
     * @return
     * @author rongjianmin
     */
    public static BigDecimal bgNullTo0(BigDecimal bgvalue) {
        if (bgvalue == null || "".equals(bgvalue)) {
            bgvalue = new BigDecimal("0");
        }
        return bgvalue;
    }

    /**
     * 返回Long类型毫秒日期
     */
    public static Long dateStrToLong(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        if (str != null) {
            try {
                Date d =  sdf.parse(str);
                if(d != null){
                    return d.getTime();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }



    /**
     * 根据传入的时间字符串与需要加减的月数返回日期。
     */
    public static Date dateCalc(String dateStr, String type, int n) throws ParseException {

        int t = 0;

        if ("m".equals(type)) {
            t = Calendar.MONTH;
        } else if ("y".equals(type)) {
            t = Calendar.YEAR;
        } else {
            t = Calendar.DATE;
        }

        dateStr = dateStr.replaceAll("/", "-");

        Date date = getDateFromStr(dateStr);
        Calendar dateCal = Calendar.getInstance();
        dateCal.setTime(date);
        dateCal.add(t, n);
        date = dateCal.getTime();

        return date;

    }

    /**
     * 根据传入的时间与需要加减的月数返回日期。
     */
    public static Date dateCalc(Date date, String type, int n) throws ParseException {

        int t = 0;

        if ("m".equals(type)) {
            t = Calendar.MONTH;
        } else if ("y".equals(type)) {
            t = Calendar.YEAR;
        } else {
            t = Calendar.DATE;
        }
        Calendar dateCal = Calendar.getInstance();
        dateCal.setTime(date);
        dateCal.add(t, n);
        date = dateCal.getTime();
        return date;
    }

    public enum FlagScope {
        day("DAY"),
        month("MONTH");

        private String flag;

        FlagScope(String flag) {
            this.flag = flag;
        }

        public String getFlag() {
            return this.flag;
        }
    }


    /**
     * 获取SimpleDateFormat
     *
     * @param pattern
     *            日期格式
     * @return SimpleDateFormat对象
     * @throws RuntimeException
     *             异常：非法日期格式
     */
    private static SimpleDateFormat getDateFormat(String pattern) throws RuntimeException {
        SimpleDateFormat dateFormat = threadLocal.get();
        if (dateFormat == null) {
            synchronized (object) {
                if (dateFormat == null) {
                    dateFormat = new SimpleDateFormat(pattern);
                    dateFormat.setLenient(false);
                    threadLocal.set(dateFormat);
                }
            }
        }
        dateFormat.applyPattern(pattern);
        return dateFormat;
    }

    /**
     * 获取日期中的某数值。如获取月份
     *
     * @param date
     *            日期
     * @param dateType
     *            日期格式
     * @return 数值
     */
    private static int getInteger(Date date, int dateType) {
        int num = 0;
        Calendar calendar = Calendar.getInstance();
        if (date != null) {
            calendar.setTime(date);
            num = calendar.get(dateType);
        }
        return num;
    }

    /**
     * 增加日期中某类型的某数值。如增加日期
     *
     * @param date
     *            日期字符串
     * @param dateType
     *            类型
     * @param amount
     *            数值
     * @return 计算后日期字符串
     */
    private static String addInteger(String date, int dateType, int amount) {
        String dateString = null;
        DateStyle dateStyle = getDateStyle(date);
        if (dateStyle != null) {
            Date myDate = StringToDate(date, dateStyle);
            myDate = addInteger(myDate, dateType, amount);
            dateString = DateToString(myDate, dateStyle);
        }
        return dateString;
    }

    /**
     * 增加日期中某类型的某数值。如增加日期
     *
     * @param date
     *            日期
     * @param dateType
     *            类型
     * @param amount
     *            数值
     * @return 计算后日期
     */
    private static Date addInteger(Date date, int dateType, int amount) {
        Date myDate = null;
        if (date != null) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(dateType, amount);
            myDate = calendar.getTime();
        }
        return myDate;
    }

    /**
     * 获取精确的日期
     *
     * @param timestamps
     *            时间long集合
     * @return 日期
     */
    private static Date getAccurateDate(List<Long> timestamps) {
        Date date = null;
        long timestamp = 0;
        Map<Long, long[]> map = new HashMap<Long, long[]>();
        List<Long> absoluteValues = new ArrayList<Long>();

        if (timestamps != null && timestamps.size() > 0) {
            if (timestamps.size() > 1) {
                for (int i = 0; i < timestamps.size(); i++) {
                    for (int j = i + 1; j < timestamps.size(); j++) {
                        long absoluteValue = Math.abs(timestamps.get(i) - timestamps.get(j));
                        absoluteValues.add(absoluteValue);
                        long[] timestampTmp = { timestamps.get(i), timestamps.get(j) };
                        map.put(absoluteValue, timestampTmp);
                    }
                }

                // 有可能有相等的情况。如2012-11和2012-11-01。时间戳是相等的。此时minAbsoluteValue为0
                // 因此不能将minAbsoluteValue取默认值0
                long minAbsoluteValue = -1;
                if (!absoluteValues.isEmpty()) {
                    minAbsoluteValue = absoluteValues.get(0);
                    for (int i = 1; i < absoluteValues.size(); i++) {
                        if (minAbsoluteValue > absoluteValues.get(i)) {
                            minAbsoluteValue = absoluteValues.get(i);
                        }
                    }
                }

                if (minAbsoluteValue != -1) {
                    long[] timestampsLastTmp = map.get(minAbsoluteValue);

                    long dateOne = timestampsLastTmp[0];
                    long dateTwo = timestampsLastTmp[1];
                    if (absoluteValues.size() > 1) {
                        timestamp = Math.abs(dateOne) > Math.abs(dateTwo) ? dateOne : dateTwo;
                    }
                }
            } else {
                timestamp = timestamps.get(0);
            }
        }

        if (timestamp != 0) {
            date = new Date(timestamp);
        }
        return date;
    }

    /**
     * 判断字符串是否为日期字符串
     *
     * @param date
     *            日期字符串
     * @return true or false
     */
    public static boolean isDate(String date) {
        boolean isDate = false;
        if (date != null) {
            if (getDateStyle(date) != null) {
                isDate = true;
            }
        }
        return isDate;
    }

    /**
     * 获取日期字符串的日期风格。失敗返回null。
     *
     * @param date
     *            日期字符串
     * @return 日期风格
     */
    public static DateStyle getDateStyle(String date) {
        DateStyle dateStyle = null;
        Map<Long, DateStyle> map = new HashMap<Long, DateStyle>();
        List<Long> timestamps = new ArrayList<Long>();
        for (DateStyle style : DateStyle.values()) {
            if (style.isShowOnly()) {
                continue;
            }
            Date dateTmp = null;
            if (date != null) {
                try {
                    ParsePosition pos = new ParsePosition(0);
                    dateTmp = getDateFormat(style.getValue()).parse(date, pos);
                    if (pos.getIndex() != date.length()) {
                        dateTmp = null;
                    }
                } catch (Exception e) {
                }
            }
            if (dateTmp != null) {
                timestamps.add(dateTmp.getTime());
                map.put(dateTmp.getTime(), style);
            }
        }
        Date accurateDate = getAccurateDate(timestamps);
        if (accurateDate != null) {
            dateStyle = map.get(accurateDate.getTime());
        }
        return dateStyle;
    }

    /**
     * 将日期字符串转化为日期。失败返回null。
     *
     * @param date
     *            日期字符串
     * @return 日期
     */
    public static Date StringToDate(String date) {
        DateStyle dateStyle = getDateStyle(date);
        return StringToDate(date, dateStyle);
    }

    /**
     * 将日期字符串转化为日期。失败返回null。
     *
     * @param date
     *            日期字符串
     * @param pattern
     *            日期格式
     * @return 日期
     */
    public static Date StringToDate(String date, String pattern) {
        Date myDate = null;
        if (date != null) {
            try {
                myDate = getDateFormat(pattern).parse(date);
            } catch (Exception e) {
            }
        }
        return myDate;
    }

    /**
     * 将日期字符串转化为日期。失败返回null。
     *
     * @param date
     *            日期字符串
     * @param dateStyle
     *            日期风格
     * @return 日期
     */
    public static Date StringToDate(String date, DateStyle dateStyle) {
        Date myDate = null;
        if (dateStyle != null) {
            myDate = StringToDate(date, dateStyle.getValue());
        }
        return myDate;
    }

    /**
     * 将日期转化为日期字符串。失败返回null。
     *
     * @param date
     *            日期
     * @param pattern
     *            日期格式
     * @return 日期字符串
     */
    public static String DateToString(Date date, String pattern) {
        String dateString = null;
        if (date != null) {
            try {
                dateString = getDateFormat(pattern).format(date);
            } catch (Exception e) {
            }
        }
        return dateString;
    }

    /**
     * 将日期转化为日期字符串。失败返回null。
     *
     * @param date
     *            日期
     * @param dateStyle
     *            日期风格
     * @return 日期字符串
     */
    public static String DateToString(Date date, DateStyle dateStyle) {
        String dateString = null;
        if (dateStyle != null) {
            dateString = DateToString(date, dateStyle.getValue());
        }
        return dateString;
    }

    /**
     * 将日期字符串转化为另一日期字符串。失败返回null。
     *
     * @param date
     *            旧日期字符串
     * @param newPattern
     *            新日期格式
     * @return 新日期字符串
     */
    public static String StringToString(String date, String newPattern) {
        DateStyle oldDateStyle = getDateStyle(date);
        return StringToString(date, oldDateStyle, newPattern);
    }

    /**
     * 将日期字符串转化为另一日期字符串。失败返回null。
     *
     * @param date
     *            旧日期字符串
     * @param newDateStyle
     *            新日期风格
     * @return 新日期字符串
     */
    public static String StringToString(String date, DateStyle newDateStyle) {
        DateStyle oldDateStyle = getDateStyle(date);
        return StringToString(date, oldDateStyle, newDateStyle);
    }

    /**
     * 将日期字符串转化为另一日期字符串。失败返回null。
     *
     * @param date
     *            旧日期字符串
     * @param olddPattern
     *            旧日期格式
     * @param newPattern
     *            新日期格式
     * @return 新日期字符串
     */
    public static String StringToString(String date, String olddPattern, String newPattern) {
        return DateToString(StringToDate(date, olddPattern), newPattern);
    }

    /**
     * 将日期字符串转化为另一日期字符串。失败返回null。
     *
     * @param date
     *            旧日期字符串
     * @param olddDteStyle
     *            旧日期风格
     * @param newParttern
     *            新日期格式
     * @return 新日期字符串
     */
    public static String StringToString(String date, DateStyle olddDteStyle, String newParttern) {
        String dateString = null;
        if (olddDteStyle != null) {
            dateString = StringToString(date, olddDteStyle.getValue(), newParttern);
        }
        return dateString;
    }

    /**
     * 将日期字符串转化为另一日期字符串。失败返回null。
     *
     * @param date
     *            旧日期字符串
     * @param olddPattern
     *            旧日期格式
     * @param newDateStyle
     *            新日期风格
     * @return 新日期字符串
     */
    public static String StringToString(String date, String olddPattern, DateStyle newDateStyle) {
        String dateString = null;
        if (newDateStyle != null) {
            dateString = StringToString(date, olddPattern, newDateStyle.getValue());
        }
        return dateString;
    }

    /**
     * 将日期字符串转化为另一日期字符串。失败返回null。
     *
     * @param date
     *            旧日期字符串
     * @param olddDteStyle
     *            旧日期风格
     * @param newDateStyle
     *            新日期风格
     * @return 新日期字符串
     */
    public static String StringToString(String date, DateStyle olddDteStyle, DateStyle newDateStyle) {
        String dateString = null;
        if (olddDteStyle != null && newDateStyle != null) {
            dateString = StringToString(date, olddDteStyle.getValue(), newDateStyle.getValue());
        }
        return dateString;
    }

    /**
     * 增加日期的年份。失败返回null。
     *
     * @param date
     *            日期
     * @param yearAmount
     *            增加数量。可为负数
     * @return 增加年份后的日期字符串
     */
    public static String addYear(String date, int yearAmount) {
        return addInteger(date, Calendar.YEAR, yearAmount);
    }

    /**
     * 增加日期的年份。失败返回null。
     *
     * @param date
     *            日期
     * @param yearAmount
     *            增加数量。可为负数
     * @return 增加年份后的日期
     */
    public static Date addYear(Date date, int yearAmount) {
        return addInteger(date, Calendar.YEAR, yearAmount);
    }

    /**
     * 增加日期的月份。失败返回null。
     *
     * @param date
     *            日期
     * @param monthAmount
     *            增加数量。可为负数
     * @return 增加月份后的日期字符串
     */
    public static String addMonth(String date, int monthAmount) {
        return addInteger(date, Calendar.MONTH, monthAmount);
    }

    /**
     * 增加日期的月份。失败返回null。
     *
     * @param date
     *            日期
     * @param monthAmount
     *            增加数量。可为负数
     * @return 增加月份后的日期
     */
    public static Date addMonth(Date date, int monthAmount) {
        return addInteger(date, Calendar.MONTH, monthAmount);
    }

    /**
     * 增加日期的天数。失败返回null。
     *
     * @param date
     *            日期字符串
     * @param dayAmount
     *            增加数量。可为负数
     * @return 增加天数后的日期字符串
     */
    public static String addDay(String date, int dayAmount) {
        return addInteger(date, Calendar.DATE, dayAmount);
    }

    /**
     * 增加日期的天数。失败返回null。
     *
     * @param date
     *            日期
     * @param dayAmount
     *            增加数量。可为负数
     * @return 增加天数后的日期
     */
    public static Date addDay(Date date, int dayAmount) {
        return addInteger(date, Calendar.DATE, dayAmount);
    }

    /**
     * 增加日期的小时。失败返回null。
     *
     * @param date
     *            日期字符串
     * @param hourAmount
     *            增加数量。可为负数
     * @return 增加小时后的日期字符串
     */
    public static String addHour(String date, int hourAmount) {
        return addInteger(date, Calendar.HOUR_OF_DAY, hourAmount);
    }

    /**
     * 增加日期的小时。失败返回null。
     *
     * @param date
     *            日期
     * @param hourAmount
     *            增加数量。可为负数
     * @return 增加小时后的日期
     */
    public static Date addHour(Date date, int hourAmount) {
        return addInteger(date, Calendar.HOUR_OF_DAY, hourAmount);
    }

    /**
     * 增加日期的分钟。失败返回null。
     *
     * @param date
     *            日期字符串
     * @param minuteAmount
     *            增加数量。可为负数
     * @return 增加分钟后的日期字符串
     */
    public static String addMinute(String date, int minuteAmount) {
        return addInteger(date, Calendar.MINUTE, minuteAmount);
    }

    /**
     * 增加日期的分钟。失败返回null。
     *
     * @param date
     *            日期
     * @param minuteAmount
     *            增加数量。可为负数
     * @return 增加分钟后的日期
     */
    public static Date addMinute(Date date, int minuteAmount) {
        return addInteger(date, Calendar.MINUTE, minuteAmount);
    }

    /**
     * 增加日期的秒钟。失败返回null。
     *
     * @param date
     *            日期字符串
     * @param secondAmount
     *            增加数量。可为负数
     * @return 增加秒钟后的日期字符串
     */
    public static String addSecond(String date, int secondAmount) {
        return addInteger(date, Calendar.SECOND, secondAmount);
    }

    /**
     * 增加日期的秒钟。失败返回null。
     *
     * @param date
     *            日期
     * @param secondAmount
     *            增加数量。可为负数
     * @return 增加秒钟后的日期
     */
    public static Date addSecond(Date date, int secondAmount) {
        return addInteger(date, Calendar.SECOND, secondAmount);
    }

    /**
     * 获取日期的年份。失败返回0。
     *
     * @param date
     *            日期字符串
     * @return 年份
     */
    public static int getYear(String date) {
        return getYear(StringToDate(date));
    }

    /**
     * 获取日期的年份。失败返回0。
     *
     * @param date
     *            日期
     * @return 年份
     */
    public static int getYear(Date date) {
        return getInteger(date, Calendar.YEAR);
    }

    /**
     * 获取日期的月份。失败返回0。
     *
     * @param date
     *            日期字符串
     * @return 月份
     */
    public static int getMonth(String date) {
        return getMonth(StringToDate(date));
    }

    /**
     * 获取日期的月份。失败返回0。
     *
     * @param date
     *            日期
     * @return 月份
     */
    public static int getMonth(Date date) {
        return getInteger(date, Calendar.MONTH) + 1;
    }

    /**
     * 获取日期的天数。失败返回0。
     *
     * @param date
     *            日期字符串
     * @return 天
     */
    public static int getDay(String date) {
        return getDay(StringToDate(date));
    }

    /**
     * 获取日期的天数。失败返回0。
     *
     * @param date
     *            日期
     * @return 天
     */
    public static int getDay(Date date) {
        return getInteger(date, Calendar.DATE);
    }

    /**
     * 获取日期的小时。失败返回0。
     *
     * @param date
     *            日期字符串
     * @return 小时
     */
    public static int getHour(String date) {
        return getHour(StringToDate(date));
    }

    /**
     * 获取日期的小时。失败返回0。
     *
     * @param date
     *            日期
     * @return 小时
     */
    public static int getHour(Date date) {
        return getInteger(date, Calendar.HOUR_OF_DAY);
    }

    /**
     * 获取日期的分钟。失败返回0。
     *
     * @param date
     *            日期字符串
     * @return 分钟
     */
    public static int getMinute(String date) {
        return getMinute(StringToDate(date));
    }

    /**
     * 获取日期的分钟。失败返回0。
     *
     * @param date
     *            日期
     * @return 分钟
     */
    public static int getMinute(Date date) {
        return getInteger(date, Calendar.MINUTE);
    }

    /**
     * 获取日期的秒钟。失败返回0。
     *
     * @param date
     *            日期字符串
     * @return 秒钟
     */
    public static int getSecond(String date) {
        return getSecond(StringToDate(date));
    }

    /**
     * 获取日期的秒钟。失败返回0。
     *
     * @param date
     *            日期
     * @return 秒钟
     */
    public static int getSecond(Date date) {
        return getInteger(date, Calendar.SECOND);
    }

    /**
     * 获取日期 。默认yyyy-MM-dd格式。失败返回null。
     *
     * @param date
     *            日期字符串
     * @return 日期
     */
    public static String getDate(String date) {
        return StringToString(date, DateStyle.YYYY_MM_DD);
    }

    /**
     * 获取日期。默认yyyy-MM-dd格式。失败返回null。
     *
     * @param date
     *            日期
     * @return 日期
     */
    public static String getDate(Date date) {
        return DateToString(date, DateStyle.YYYY_MM_DD);
    }

    public static String getDateTime(String date) {
        String reg = "(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})";
        return date.replaceAll(reg, "$1-$2-$3 $4:$5:$6");
    }
    public static String getDateMMddyyyy(String date) {
        String newD="";
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date date2 = format.parse(date);//有异常要捕获
            format = new SimpleDateFormat("yyyyMMddhhmmss");
            newD = format.format(date2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return newD;
    }
    /*
     * @param date
     * @return
     */
    public static String getDateTime(Date date) {
        return DateToString(date, DateStyle.YYYY_MM_DD_HH_MM_SSS);
    }
    /*
     * @param date
     * @return
     */
    public static String getDateStyle(Date date) {
        return DateToString(date, DateStyle.YYYY_MM_DD_HH_MM_SS);
    }
    /**
     * 获取日期。默认yyyy-MM格式。失败返回null。
     *
     * @param date
     *            日期
     * @return 日期
     */
    public static String getStrMonth(Date date) {
        return DateToString(date, DateStyle.YYYY_MM);
    }

    /**
     * 获取日期的时间。默认HH:mm:ss格式。失败返回null。
     *
     * @param date
     *            日期字符串
     * @return 时间
     */
    public static String getTime(String date) {
        return StringToString(date, DateStyle.HH_MM_SS);
    }

    /**
     * 获取日期的时间。默认HH:mm:ss格式。失败返回null。
     *
     * @param date
     *            日期
     * @return 时间
     */
    public static String getTime(Date date) {
        return DateToString(date, DateStyle.HH_MM_SS);
    }

    /**
     * 获取日期的星期。失败返回null。
     *
     * @param date
     *            日期字符串
     * @return 星期
     */
    public static Week getWeek(String date) {
        Week week = null;
        DateStyle dateStyle = getDateStyle(date);
        if (dateStyle != null) {
            Date myDate = StringToDate(date, dateStyle);
            week = getWeek(myDate);
        }
        return week;
    }

    /**
     * 获取日期的星期。失败返回null。
     *
     * @param date
     *            日期
     * @return 星期
     */
    public static Week getWeek(Date date) {
        Week week = null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int weekNumber = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        switch (weekNumber) {
            case 0:
                week = Week.SUNDAY;
                break;
            case 1:
                week = Week.MONDAY;
                break;
            case 2:
                week = Week.TUESDAY;
                break;
            case 3:
                week = Week.WEDNESDAY;
                break;
            case 4:
                week = Week.THURSDAY;
                break;
            case 5:
                week = Week.FRIDAY;
                break;
            case 6:
                week = Week.SATURDAY;
                break;
        }
        return week;
    }

    /**
     * 获取两个日期相差的天数
     *
     * @param date
     *            日期字符串
     * @param otherDate
     *            另一个日期字符串
     * @return 相差天数。如果失败则返回-1
     */
    public static int getIntervalDays(String date, String otherDate) {
        return getIntervalDays(StringToDate(date), StringToDate(otherDate));
    }

    /**
     * @param date
     *            日期
     * @param otherDate
     *            另一个日期
     * @return 相差天数。如果失败则返回-1
     */
    public static int getIntervalDays(Date date, Date otherDate) {
        int num = -1;
        Date dateTmp = DateUtil.StringToDate(DateUtil.getDate(date), DateStyle.YYYY_MM_DD);
        Date otherDateTmp = DateUtil.StringToDate(DateUtil.getDate(otherDate), DateStyle.YYYY_MM_DD);
        if (dateTmp != null && otherDateTmp != null) {
            long time = Math.abs(dateTmp.getTime() - otherDateTmp.getTime());
            num = (int) (time / (24 * 60 * 60 * 1000));
        }
        return num;
    }

    public static BigDecimal getIntervalDaysReturnDecimal(Date date, Date otherDate) {
        BigDecimal num = new BigDecimal("0.00");
        if (date != null && otherDate != null) {
            long time = Math.abs(date.getTime() - otherDate.getTime());
            BigDecimal timeByDecimal = new BigDecimal(time);
            Long timesByDays = 24 * 60 * 60 * 1000L;
            num = timeByDecimal.divide(new BigDecimal(timesByDays.toString()), 2, BigDecimal.ROUND_HALF_UP);
        }
        return num;
    }

    /**
     * 计算两个日期之间相差的月数
     *
     * @param date
     * @param otherDate
     * @return
     */
    public static BigDecimal getMonthsReturnDecimal(Date date, Date otherDate) {
        BigDecimal num = new BigDecimal("0.00");
        if (date != null && otherDate != null) {
            long time = Math.abs(date.getTime() - otherDate.getTime());
            BigDecimal timeByDecimal = new BigDecimal(time);
            Long timesByMonths = 24 * 60 * 60 * 1000 * 30L;
            num = timeByDecimal.divide(new BigDecimal(timesByMonths.toString()), 2,
                    BigDecimal.ROUND_HALF_UP);
        }
        return num;
    }


    /**
     * @desc 比较date_1和date_2两个日期的大小，date_1是否在date_2之前或之后。
     * @param date_1
     *            一般为batchNo
     * @param date_2
     * @param type
     *            类型、年月日其中之一
     * @param counts
     *            推移的天数
     * @param tag
     *            标志位，表示是before 0还是after 1
     * @return
     */
    public static boolean isBeforeOrAfter(String date_1, Date date_2, int type, int counts, int tag){
        boolean flag;

        Calendar c1 = Calendar.getInstance();
        Calendar c2  = Calendar.getInstance();
        try {
            c1.setTime(df.parse(date_1));
        } catch (ParseException e) {
            return false;
        }
        c2.setTime(date_2);
        if (type == Calendar.MONTH) {
            c1.add(Calendar.MONTH, counts);
        }else if (type == Calendar.YEAR) {
            c1.add(Calendar.YEAR, counts);
        }else if( type == Calendar.DATE){
            c1.add(Calendar.DATE, counts);
        }

        flag = (0 == tag) ? c1.before(c2) : c1.after(c2);

        return flag;
    }
    public static boolean isBeforeOrAfter(String date_1, String date_2, int type, int counts, int tag){
        boolean flag;

        Calendar c1 = Calendar.getInstance();
        Calendar c2  = Calendar.getInstance();
        try {
            c1.setTime(df.parse(date_1));
            c2.setTime(df.parse(date_2));
        } catch (ParseException e) {
            LOG.error("============================isBeforeOrAfter.date_1: " + date_1, e);
            return false;
        }
        if (type == Calendar.MONTH) {
            c1.add(Calendar.MONTH, counts);
        }else if (type == Calendar.YEAR) {
            c1.add(Calendar.YEAR, counts);
        }else if( type == Calendar.DATE){
            c1.add(Calendar.DATE, counts);
        }

        flag = (0 == tag) ? c1.before(c2) : c1.after(c2);

        return flag;
    }
    public static Date timeToDate(Long time) {

        Timestamp timestamp = new Timestamp(time);
        // Timestamp -> String
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(timestamp);

        // String -> Date
        Date date = new Date();
        //注意format的格式要与日期String的格式相匹配
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }
    /**
     * 获取x月x日前日期
     *
     * @author jiaofei
     * @param months
     * @param days
     * @return
     * @throws ParseException
     * @throws Exception
     */
    public static Date getDateByMonthorDay(String date, int months, int days) throws ParseException {
        Date recentNCycle = DateUtil.dateCalc(DateUtil.dateCalc(date, "m", months), "d", days);
        return recentNCycle;
    }

    /**
     * timestamp转换为Calendar
     * @param timestamp
     * @return
     * @throws ParseException
     * @author fansl
     */
    public static Calendar timestamp2calendar(Timestamp timestamp) throws ParseException {
        SimpleDateFormat sdf=new SimpleDateFormat(DateStyle.YYYY_MM_DD_HH_MM_SS.getValue());
        String currentDateStr=sdf.format(timestamp);

        Date date=sdf.parse(currentDateStr);
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(date);
        return calendar;
    }

    public static void main(String[] args) throws Exception {
        Date dd = new Date();
        //System.out.println(DateToString(new Date(),DateStyle.YYYY_MM_DD_HH_MM_SS));

        long t=1570760284000L;
        Timestamp timestamp = new Timestamp(t);
        //System.out.println(DateToString(timeToDate(timestamp),DateStyle.YYYY_MM_DD_HH_MM_SS));

        // System.out.println(getDateMMddyyyy("2020-05-25 09:30:10"));
        System.out.println(getMinute("2020-05-25 09:30:10"));
        System.out.println(getMinute("20200525083000"));


        String string = "aaa456ac";
        //查找指定字符是在字符串中的下标。在则返回所在字符串下标；不在则返回-1.
        //System.out.println(string.indexOf("aa")); // indexOf(String str); 返回结果：-1，"b"不存在


    }
}
