/**
 * FileName: dataUtils
 * Author:   DFJX
 * Date:     2019/8/6 14:50
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.fyc.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author DFJX
 * @create 2019/8/6
 * @since 1.0.0
 */
public class dateUtils {

    static int count = 179;


    public static void main(String[] args)throws Exception {
        System.out.println(date2WeekN("2020-01-02", 5));
    }

    public static int getUnixTimestamp(){
        String dateTemp = (new Date().getTime() + "").substring(0, 10);

        return  Integer.parseInt(dateTemp);
    }

    public static Date toSimpleDate(String s) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        if (s == null) {
            return null;
        }
        try {
            return sdf.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static java.sql.Date toSqlDate(String s) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        if (s == null) {
            return null;
        }
        try {
            return new java.sql.Date(sdf.parse(s).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    //将时间转换为时间戳
    public static String dateToStamp(String s) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String res;
        //设置时间格式，将该时间格式的时间转换为时间戳
        Date date = sdf.parse(s);
        long time = date.getTime();
        res = String.valueOf(time);
        return res;
    }
    //将时间戳转换为时间
    public static String stampToDate(String s) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String res;
        long lt = new Long(s);
        //将时间戳转换为时间
        Date date = new Date(lt);
        //将时间调整为yyyy-MM-dd HH:mm:ss时间样式
        res = sdf.format(date);
        return res;
    }

    public static String date2WeekN(String s,int num)throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date date = toSimpleDate(s);
        Calendar calendar =Calendar.getInstance();

        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_WEEK,num+1);
        String tmp = calendar.get(Calendar.YEAR) + "-" + (calendar.get(Calendar.MONTH) + 1) + "-" + calendar.get(Calendar.DAY_OF_MONTH);
        Date parse = sdf.parse(tmp);

        String format = sdf.format(sdf.parse(tmp));
        return format;

    }

    public static String dateAddFrequence(String s,long frequence)throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date date = sdf.parse(s);
        long timestamp = date.getTime();
        timestamp = timestamp + frequence * 60 * 60 * 24 * 1000;
        return sdf.format(timestamp);
    }

    public static int getDistanceTime(String s) throws Exception{
        int days = 0;
        String begin = dateToStamp(s);
        long end = new Date().getTime();


        days = (int) ((end - Long.valueOf(begin)) / (24 * 60 * 60 * 1000));
        return days;
    }
    public static String getYesterTimestamp()throws Exception{

        return dateToStamp(getYesterday());

    }
    //获取昨天日期
    public static String getYesterday(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Calendar calendar=Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY,-24);
        String yesterdayDate=sdf.format(calendar.getTime());
        return yesterdayDate;
    }
    //获取昨天日期
    public static String getToday(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Calendar calendar=Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY,0);
        String today=sdf.format(calendar.getTime());
        return today;
    }

    public static String getSzwBegin()throws Exception{
        Calendar now =Calendar.getInstance();

        now.set(Calendar.DATE,now.get(Calendar.DATE)-count);

        String s = stampToDate(String.valueOf(now.getTime().getTime()));
        return s;
    }

    public static String getLastWeekN(int num) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date date = new Date(new Date().getTime() - 7 * 60 * 60 * 24 * 1000);
        String format = sdf.format(date);
        return date2WeekN(format,num);

    }
    public static String date2LastWeekN(String date,int num) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String format = sdf.format(new Date(sdf.parse(date).getTime() - 7 * 60 * 60 * 24 * 1000));
        return date2WeekN(format,num);

    }

    public static String getLastMonthFirst() {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");

        Calendar calendar1=Calendar.getInstance();

        calendar1.add(Calendar.MONTH, -1);
        calendar1.set(Calendar.DAY_OF_MONTH,1);


        String first = sdf.format(calendar1.getTime());

        return first;

    }
    public static String getLastMonthEnd() {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");

        Calendar calendar2=Calendar.getInstance();


        calendar2.set(Calendar.DAY_OF_MONTH, 0);

        String last = sdf.format(calendar2.getTime());

        return last;

    }
    public static boolean isWeekN(String s,int n)throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(s);
        Calendar c=Calendar.getInstance();
        c.setTime(date);
        int weekday=c.get(Calendar.DAY_OF_WEEK)-1;
        System.out.println(weekday);
        return weekday==n;
    }
    public static String date2MonthEnd(String date)throws Exception{

        //设置时间格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        //获得实体类
        Calendar ca = Calendar.getInstance();
        ca.setTime(sdf.parse(date));
        //设置最后一天
        ca.set(Calendar.DAY_OF_MONTH, ca.getActualMaximum(Calendar.DAY_OF_MONTH));
        //最后一天格式化
        String end = sdf.format(ca.getTime());


        return end;
    }

}
