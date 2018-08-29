package com.kevin.mapreduce.utils.date;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * @desc:时间处理工具类
 * 
 * @Author:chenssy
 * @date:2014年8月4日
 */
public class DateUtils {
	private static final String[] weeks = {"星期日","星期一","星期二","星期三","星期四","星期五","星期六"};
	/**
	 * 根据指定格式获取当前时间
	 * @author chenssy
	 * @date Dec 27, 2013
	 * @param format
	 * @return String
	 */
	public static String getCurrentTime(String format){
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		Date date = new Date();
		return sdf.format(date);
	}
	
	/**
	 * 获取当前时间，格式为：yyyy-MM-dd HH:mm:ss
	 * @author chenssy
	 * @date Dec 27, 2013
	 * @return String
	 */
	public static String getCurrentTime(){
		return getCurrentTime(DateFormatUtils.DATE_FORMAT2);
	}
	
	/**
	 * 获取指定格式的当前时间：为空时格式为yyyy-mm-dd HH:mm:ss
	 * @author chenssy
	 * @date Dec 30, 2013
	 * @param format
	 * @return Date
	 */
	public static Date getCurrentDate(String format){
		 SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		 String dateS = getCurrentTime(format);
		 Date date = null;
		 try {
			date = sdf.parse(dateS);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	
	/**
	 * 获取当前时间，格式为yyyy-MM-dd HH:mm:ss
	 * @author chenssy
	 * @date Dec 30, 2013
	 * @return Date
	 */
	public static Date getCurrentDate(){
		return getCurrentDate(DateFormatUtils.DATE_FORMAT2);
	}
	
	/**
	 * 给指定日期加入年份，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 30, 2013
	 * @param year 年份  正数相加、负数相减
	 * @param date 为空时，默认为当前时间
	 * @param format 默认格式为：yyyy-MM-dd HH:mm:ss
	 * @return String
	 */
	public static String addYearToDate(int year,Date date,String format){
		Calendar calender = getCalendar(date,format);
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		
		calender.add(Calendar.YEAR, year);
		
		return sdf.format(calender.getTime());
	}
	
	/**
	 * 给指定日期加入年份，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 30, 2013
	 * @param year 年份  正数相加、负数相减
	 * @param date 为空时，默认为当前时间
	 * @param format 默认格式为：yyyy-MM-dd HH:mm:ss
	 * @return String
	 */
	public static String addYearToDate(int year,String date,String format){
		Date newDate = new Date();
		if(null != date && !"".equals(date)){
			newDate = string2Date(date, format);
		}
		
		return addYearToDate(year, newDate, format);
	}
	
	/**
	 * 给指定日期增加月份 为空时默认当前时间
	 * @author chenssy
	 * @date Dec 30, 2013
	 * @param month  增加月份  正数相加、负数相减
	 * @param date 指定时间
	 * @param format 指定格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addMothToDate(int month,Date date,String format) {
		Calendar calender = getCalendar(date,format);
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		
		calender.add(Calendar.MONTH, month);
		
		return sdf.format(calender.getTime());
	}
	
	/**
	 * 给指定日期增加月份 为空时默认当前时间
	 * @author chenssy
	 * @date Dec 30, 2013
	 * @param month  增加月份  正数相加、负数相减
	 * @param date 指定时间
	 * @param format 指定格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addMothToDate(int month,String date,String format) {
		Date newDate = new Date();
		if(null != date && !"".equals(date)){
			newDate = string2Date(date, format);
		}
		
		return addMothToDate(month, newDate, format);
	}
	
	/**
	 * 给指定日期增加天数，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param day 增加天数 正数相加、负数相减
	 * @param date 指定日期
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addDayToDate(int day,Date date,String format) {
		Calendar calendar = getCalendar(date, format);
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		
		calendar.add(Calendar.DATE, day);
		
		return sdf.format(calendar.getTime());
	}
	
	/**
	 * 给指定日期增加天数，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param day 增加天数 正数相加、负数相减
	 * @param date 指定日期
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addDayToDate(int day,String date,String format) {
		Date newDate = new Date();
		if(null != date && !"".equals(date)){
			newDate = string2Date(date, format);
		}
		
		return addDayToDate(day, newDate, format);
	}
	
	/**
	 * 给指定日期增加小时，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param hour 增加小时  正数相加、负数相减
	 * @param date 指定日期
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addHourToDate(int hour,Date date,String format) {
		Calendar calendar = getCalendar(date, format);
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		
		calendar.add(Calendar.HOUR, hour);
		
		return sdf.format(calendar.getTime());
	}
	
	/**
	 * 给指定日期增加小时，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param hour 增加小时  正数相加、负数相减
	 * @param date 指定日期
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addHourToDate(int hour,String date,String format) {
		Date newDate = new Date();
		if(null != date && !"".equals(date)){
			newDate = string2Date(date, format);
		}
		
		return addHourToDate(hour, newDate, format);
	}
	
	/**
	 * 给指定的日期增加分钟，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param minute 增加分钟  正数相加、负数相减
	 * @param date 指定日期 
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addMinuteToDate(int minute,Date date,String format) {
		Calendar calendar = getCalendar(date, format);
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		
		calendar.add(Calendar.MINUTE, minute);
		
		return sdf.format(calendar.getTime());
	}
	
	/**
	 * 给指定的日期增加分钟，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param minute 增加分钟  正数相加、负数相减
	 * @param date 指定日期 
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addMinuteToDate(int minute,String date,String format){
		Date newDate = new Date();
		if(null != date && !"".equals(date)){
			newDate = string2Date(date, format);
		}
		
		return addMinuteToDate(minute, newDate, format);
	}
	
	/**
	 * 给指定日期增加秒，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param second 增加秒 正数相加、负数相减
	 * @param date 指定日期
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 */
	public static String addSecondToDate(int second,Date date,String format){
		Calendar calendar = getCalendar(date, format);
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		
		calendar.add(Calendar.SECOND, second);
		
		return sdf.format(calendar.getTime());
	}
	
	/**
	 * 给指定日期增加秒，为空时默认当前时间
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param second 增加秒 正数相加、负数相减
	 * @param date 指定日期
	 * @param format 日期格式 为空默认 yyyy-mm-dd HH:mm:ss
	 * @return String
	 * @throws Exception 
	 */
	public static String addSecondToDate(int second,String date,String format){
		Date newDate = new Date();
		if(null != date && !"".equals(date)){
			newDate = string2Date(date, format);
		}
		
		return addSecondToDate(second, newDate, format);
	}
	
	/**
	 * 获取指定格式指定时间的日历
	 * @author chenssy
	 * @date Dec 30, 2013
	 * @param date 时间 
	 * @param format 格式
	 * @return Calendar
	 */
	public static Calendar getCalendar(Date date,String format){
		if(date == null){
			date = getCurrentDate(format);
		}
		
		Calendar calender = Calendar.getInstance();
		calender.setTime(date);
		
		return calender;
	}
	
	/**
	 * 字符串转换为日期，日期格式为
	 * 
	 * @author : chenssy
	 * @date : 2016年5月31日 下午5:20:22
	 *
	 * @param value
	 * @return
	 */
	public static Date string2Date(String value){
		if(value == null || "".equals(value)){
			return null;
		}
		
		SimpleDateFormat sdf = DateFormatUtils.getFormat(DateFormatUtils.DATE_FORMAT2);
		Date date = null;
		
		try {
			value = DateFormatUtils.formatDate(value, DateFormatUtils.DATE_FORMAT2);
			date = sdf.parse(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return date;
	}
	
	/**
	 * 将字符串(格式符合规范)转换成Date
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 需要转换的字符串
	 * @param format 日期格式 
	 * @return Date
	 */
	public static Date string2Date(String value,String format){
		if(value == null || "".equals(value)){
			return null;
		}
		
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		Date date = null;
		
		try {
			value = DateFormatUtils.formatDate(value, format);
			date = sdf.parse(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return date;
	}
	
	/**
	 * 将日期格式转换成String
	 * @author chenssy
	 * @date Dec 31, 2013
	 * 
	 * @param value 需要转换的日期
	 * @param format 日期格式
	 * @return String
	 */
	public static String date2String(Date value,String format){
		if(value == null){
			return null;
		}
		
		SimpleDateFormat sdf = DateFormatUtils.getFormat(format);
		return sdf.format(value);
	}
	
	/**
	 * 日期转换为字符串
	 * 
	 * @author : chenssy
	 * @date : 2016年5月31日 下午5:21:38
	 *
	 * @param value
	 * @return
	 */
	public static String date2String(Date value){
		if(value == null){
			return null;
		}
		
		SimpleDateFormat sdf = DateFormatUtils.getFormat(DateFormatUtils.DATE_FORMAT2);
		return sdf.format(value);
	}
	
	/**
	 * 获取指定日期的年份
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentYear(Date value){
		String date = date2String(value, DateFormatUtils.DATE_YEAR);
		return Integer.valueOf(date);
	}
	
	/**
	 * 获取指定日期的年份
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentYear(String value) {
		Date date = string2Date(value, DateFormatUtils.DATE_YEAR);
		Calendar calendar = getCalendar(date, DateFormatUtils.DATE_YEAR);
		return calendar.get(Calendar.YEAR);
	}
	
	/**
	 * 获取指定日期的月份
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentMonth(Date value){
		String date = date2String(value, DateFormatUtils.DATE_MONTH);
		return Integer.valueOf(date);
	}
	
	/**
	 * 获取指定日期的月份
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentMonth(String value) {
		Date date = string2Date(value, DateFormatUtils.DATE_MONTH);
		Calendar calendar = getCalendar(date, DateFormatUtils.DATE_MONTH);
		
		return calendar.get(Calendar.MONTH);
	}
	
	/**
	 * 获取指定日期的天份
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentDay(Date value){
		String date = date2String(value, DateFormatUtils.DATE_DAY);
		return Integer.valueOf(date);
	}
	
	/**
	 * 获取指定日期的天份
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentDay(String value){
		Date date = string2Date(value, DateFormatUtils.DATE_DAY);
		Calendar calendar = getCalendar(date, DateFormatUtils.DATE_DAY);
		
		return calendar.get(Calendar.DATE);
	}
	
	/**
	 * 获取当前日期为星期几
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return String
	 */
	public static String getCurrentWeek(Date value) {
		Calendar calendar = getCalendar(value, DateFormatUtils.DATE_FORMAT1);
		int weekIndex = calendar.get(Calendar.DAY_OF_WEEK) - 1 < 0 ? 0 : calendar.get(Calendar.DAY_OF_WEEK) - 1;
		
		return weeks[weekIndex];
	}
	
	/**
	 * 获取当前日期为星期几
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return String
	 */
	public static String getCurrentWeek(String value) {
		Date date = string2Date(value, DateFormatUtils.DATE_FORMAT1);
		return getCurrentWeek(date);
	}
	
	/**
	 * 获取指定日期的小时
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentHour(Date value){
		String date = date2String(value, DateFormatUtils.DATE_HOUR);
		return Integer.valueOf(date);
	}
	
	/**
	 * 获取指定日期的小时
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return
	 * @return int
	 */
	public static int getCurrentHour(String value) {
		Date date = string2Date(value, DateFormatUtils.DATE_HOUR);
		Calendar calendar = getCalendar(date, DateFormatUtils.DATE_HOUR);
		
		return calendar.get(Calendar.DATE);
	}
	
	/**
	 * 获取指定日期的分钟
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentMinute(Date value){
		String date = date2String(value, DateFormatUtils.DATE_MINUTE);
		return Integer.valueOf(date);
	}
	
	/**
	 * 获取指定日期的分钟
	 * @author chenssy
	 * @date Dec 31, 2013
	 * @param value 日期
	 * @return int
	 */
	public static int getCurrentMinute(String value){
		Date date = string2Date(value, DateFormatUtils.DATE_MINUTE);
		Calendar calendar = getCalendar(date, DateFormatUtils.DATE_MINUTE);
		
		return calendar.get(Calendar.MINUTE);
	}
	
	/**  
	 * 比较两个日期相隔多少天(月、年) <br>
	 * 例：<br>
	 * &nbsp;compareDate("2009-09-12", null, 0);//比较天 <br>
     * &nbsp;compareDate("2009-09-12", null, 1);//比较月 <br> 
     * &nbsp;compareDate("2009-09-12", null, 2);//比较年 <br>
     * 
	 * @author chenssy
	 * @date Dec 31, 2013 
     * @param startDay 需要比较的时间 不能为空(null),需要正确的日期格式 ,如：2009-09-12   
     * @param endDay 被比较的时间  为空(null)则为当前时间    
     * @param stype 返回值类型   0为多少天，1为多少个月，2为多少年    
     * @return int
     */    
    public static int compareDate(String startDay,String endDay,int stype) {     
        int n = 0;     
        startDay = DateFormatUtils.formatDate(startDay, "yyyy-MM-dd");
        endDay = DateFormatUtils.formatDate(endDay, "yyyy-MM-dd");
        
        String formatStyle = "yyyy-MM-dd";
        if(1 == stype){
        	formatStyle = "yyyy-MM";
        }else if(2 == stype){
        	formatStyle = "yyyy";
        }   
             
        endDay = endDay==null ? getCurrentTime("yyyy-MM-dd") : endDay;     
             
        DateFormat df = new SimpleDateFormat(formatStyle);     
        Calendar c1 = Calendar.getInstance();     
        Calendar c2 = Calendar.getInstance();     
        try {     
            c1.setTime(df.parse(startDay));     
            c2.setTime(df.parse(endDay));   
        } catch (Exception e) {    
        	e.printStackTrace();
        }     
        while (!c1.after(c2)) {                   // 循环对比，直到相等，n 就是所要的结果     
            n++;     
            if(stype==1){     
                c1.add(Calendar.MONTH, 1);          // 比较月份，月份+1     
            }     
            else{     
                c1.add(Calendar.DATE, 1);           // 比较天数，日期+1     
            }     
        }     
        n = n-1;     
        if(stype==2){     
            n = (int)n/365;     
        }        
        return n;     
    }   
    
    /**
     * 比较两个时间相差多少小时(分钟、秒)
     * @author chenssy
     * @date Jan 2, 2014
     * @param startTime 需要比较的时间 不能为空，且必须符合正确格式：2012-12-12 12:12:
     * @param endTime 需要被比较的时间 若为空则默认当前时间
     * @param type 1：小时   2：分钟   3：秒
     * @return int
     */
    public static int compareTime(String startTime , String endTime , int type) {
    	//endTime是否为空，为空默认当前时间
    	if(endTime == null || "".equals(endTime)){
    		endTime = getCurrentTime();
    	}
    	
    	SimpleDateFormat sdf = DateFormatUtils.getFormat("");
    	int value = 0;
    	try {
			Date begin = sdf.parse(startTime);
			Date end = sdf.parse(endTime);
			long between = (end.getTime() - begin.getTime()) / 1000;  //除以1000转换成豪秒
			if(type == 1){   //小时
				value = (int) (between % (24 * 36000) / 3600);
			}
			else if(type == 2){
				value = (int) (between % 3600 / 60);
			}
			else if(type == 3){
				value = (int) (between % 60 / 60);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	return value;
    }
    
    /**
     * 比较两个日期的大小。<br>
     * 若date1 > date2 则返回 1<br>
     * 若date1 = date2 则返回 0<br>
     * 若date1 < date2 则返回-1
     * @autor:chenssy
     * @date:2014年9月9日
     *
     * @param date1  
     * @param date2
     * @param format  待转换的格式
     * @return 比较结果
     */
    public static int compare(String date1, String date2,String format) {
        DateFormat df = DateFormatUtils.getFormat(format);
        try {
            Date dt1 = df.parse(date1);
            Date dt2 = df.parse(date2);
            if (dt1.getTime() > dt2.getTime()) {
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }
    
    /**
     * 获取指定月份的第一天 
     * 
     * @author : chenssy
     * @date : 2016年5月31日 下午5:31:10
     *
     * @param date
     * @return
     */
    public static String getMonthFirstDate(String date){
    	date = DateFormatUtils.formatDate(date);
		return DateFormatUtils.formatDate(date, "yyyy-MM") + "-01";
    }
    
    /**
     * 获取指定月份的最后一天
     * 
     * @author : chenssy
     * @date : 2016年5月31日 下午5:32:09
     *
     * @param strdate
     * @return
     */
	public static String getMonthLastDate(String date) {
		Date strDate = DateUtils.string2Date(getMonthFirstDate(date));
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(strDate);
		calendar.add(Calendar.MONTH, 1);
		calendar.add(Calendar.DAY_OF_YEAR, -1);
		return DateFormatUtils.formatDate(calendar.getTime());
	}
	
	/**
	 * 获取所在星期的第一天
	 * 
	 * @author : chenssy
	 * @date : 2016年6月1日 下午12:38:53
	 *
	 * @param date
	 * @return
	 */
	@SuppressWarnings("static-access")
	public static Date getWeekFirstDate(Date date) {
		Calendar now = Calendar.getInstance();
		now.setTime(date);
		int today = now.get(Calendar.DAY_OF_WEEK);
		int first_day_of_week = now.get(Calendar.DATE) + 2 - today; // 星期一
		now.set(now.DATE, first_day_of_week);
		return now.getTime();
	}
	
	/**
	 * 获取所在星期的最后一天
	 * 
	 * @author : chenssy
	 * @date : 2016年6月1日 下午12:40:31
	 *
	 * @param date
	 * @return
	 */
	@SuppressWarnings("static-access")
	public static Date geWeektLastDate(Date date) {
		Calendar now = Calendar.getInstance();
		now.setTime(date);
		int today = now.get(Calendar.DAY_OF_WEEK);
		int first_day_of_week = now.get(Calendar.DATE) + 2 - today; // 星期一
		int last_day_of_week = first_day_of_week + 6; // 星期日
		now.set(now.DATE, last_day_of_week);
		return now.getTime();
	}

	/**
	 * 日期格式化，自定义输出日期格式
	 *
	 * @param date
	 *            待格式化日期
	 * @param dateFormat
	 *            目标字符串格式
	 * @return
	 */
	public static String getFormatDate(Date date, String dateFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(date);
	}

	/**
	 * 通过年月值获得Timestamp对象
	 *
	 * @param mon
	 *            年月值，YYYYMM
	 * @return 对应年月的第一天, yyyy-MM-01 00:00:00
	 */
	public static Timestamp getMon(String mon) {
		String monStr = mon.replaceAll("(\\d{4})(\\d{2})", "$1-$2-01 00:00:00");
		Timestamp t = Timestamp.valueOf(monStr);
		return t;
	}

	/**
	 * 字符串转换成日期
	 *
	 * @param str
	 *            待格式化日期字符串
	 * @param dateFormat
	 *            目标日期格式
	 * @return date
	 */
	public static Date strToDate(String str, String dateFormat) {

		SimpleDateFormat format = new SimpleDateFormat(dateFormat);
		Date date = null;
		try {
			date = format.parse(str);
		}
		catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 得到日期所属季度
	 *
	 * @param inDate
	 *            Timestamp 日期
	 * @return String 季度值
	 */
	public static String getSeason(Timestamp inDate) {
		String retValue = null;
		if (inDate != null) {
			String tpStr = inDate.toString().substring(5, 7);
			int month = Integer.parseInt(tpStr);
			if (month <= 3) {
				retValue = "1";
			} else if (month <= 6) {
				retValue = "2";
			} else if (month <= 9) {
				retValue = "3";
			} else {
				retValue = "4";
			}
		}
		return retValue;
	}

	/**
	 * 得到日期所属月份第一天，返回格式yyyy-mm-dd
	 *
	 * @param tsMon
	 *            Timestamp 日期
	 * @return String
	 */
	public static String getDateString(Timestamp tsMon) {
		return tsMon.toString().substring(0, 4) + "-" + tsMon.toString().substring(5, 7) + "-01";
	}

	/**
	 * 得到日期的数字年月 yyyymm
	 *
	 * @param tsMon
	 *            Timestamp 日期
	 * @return BigDecimal
	 */
	public static BigDecimal getNumberMonFromDate(Timestamp tsMon) {
		return new BigDecimal(tsMon.toString().substring(0, 4) + tsMon.toString().substring(5, 7));
	}

	/**
	 * 得到字符串格式日期的数字年月 yyyymm
	 *
	 * @param sMon
	 *            String 日期
	 * @return BigDecimal
	 */
	public static BigDecimal getNumberMonFromString(String sMon) {
		return new BigDecimal(sMon.toString().substring(0, 4) + sMon.toString().substring(5, 7));
	}

	/**
	 * 根据数字月份得到日期型月份
	 *
	 * @param mon
	 *            BigDecimal 年月
	 * @return Timestamp
	 */
	public static Timestamp getDateMonFromNumber(BigDecimal mon) {
		String sMon = String.valueOf(mon.intValue());
		return Timestamp.valueOf(sMon.substring(0, 4) + "-" + sMon.substring(4, 6) + "-01 00:00:00.0");
	}

	/**
	 * 根据数字月份得到日期型月份
	 *
	 * @param mon
	 *            BigDecimal 年月
	 * @return Timestamp
	 */
	public static Timestamp getDateMonFromNumber(String mon) {
		return Timestamp.valueOf(mon.substring(0, 4) + "-" + mon.substring(4, 6) + "-01 00:00:00.0");
	}

	public static final Date parseDate(String time, String formatstr) {
		return parseDate(time, formatstr, Locale.getDefault());
	}

	/**
	 * 向前向后推N天
	 * @param srcDate
	 * @param nDay	负数 向前 | 正数 向后
	 * @return
	 */
	public static final Date addDayOf(Date srcDate, int nDay) {
		return addCalendar(srcDate, Calendar.DAY_OF_MONTH, nDay);
	}

	/**
	 * <p>Discription:[向前向后推N月]</p>
	 * @param srcDate
	 * @param nMonth
	 * @return
	 * @author zhanghaibo  2014-7-28
	 */
	public static final Date addMonthOf(Date srcDate, int nMonth) {
		return addCalendar(srcDate, Calendar.MONTH, nMonth);
	}

	/**
	 * 源日期是否在目标日期之前<br>
	 *    源日期与目标日期相等返回
	 * @param srcDate
	 * @param targetDate
	 * @return true 在之前   false 在之后或相等
	 */
	public static final boolean before(Date srcDate, Date targetDate) {
		if (srcDate == null || targetDate == null) return false;
		return srcDate.before(targetDate);
	}

	/**
	 * 源日期是否在目标日期之后<br>
	 *     源日期与目标日期相等返回
	 * @param srcDate
	 * @param targetDate
	 * @return true 在之后  false 在之前或相等
	 */
	public static final boolean after(Date srcDate, Date targetDate) {
		if (srcDate == null || targetDate == null) return false;
		return srcDate.after(targetDate);
	}

	/**
	 * 判断是否同一天 (年月日)
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static final boolean isSameDay(Date date1, Date date2) {
		if (date1 == null || date2 == null) return false;
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(date1);
		Calendar cal2 = Calendar.getInstance();
		cal2.setTime(date2);
		return isSameDay(cal1, cal2);
	}
	public static final boolean isSameDay(Calendar cal1, Calendar cal2) {
		if (cal1 == null || cal2 == null) return false;
		return (cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) && cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR)
				&& cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR));
	}

	private static Date addCalendar(Date srcDate, int calendarField, int nAmount) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(srcDate);
		calendar.add(calendarField, nAmount);
		return calendar.getTime();
	}

	public static final Date parseDate(String time, String formatstr, Locale locale) {
		if (formatstr == null)
			formatstr = yyyy_MM_dd_HH_mm_ss_SSS;
		SimpleDateFormat sdf = new SimpleDateFormat(formatstr, locale);
		try {
			return sdf.parse(time);
		} catch (ParseException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * <p>
	 * Discription:[返回一天 开始时间]
	 * </p>
	 *
	 * @param time
	 * @return 2014-09-04 00:00:00
	 * @author zhanghaibo 2014-7-21
	 */
	public static final Date toZerotime(long time) {
		return new Date(toStartTime(time));
	}

	/**
	 * <p>Discription:[返回一天开始时间]</p>
	 * @param time
	 * @return
	 * @author zhanghaibo  2014-7-22
	 */
	public static final long toStartTime(long time) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(time);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTimeInMillis();
	}

	/**
	 * <p>Discription:[返回一天结束时间]</p>
	 * @param time
	 * @return
	 * @author zhanghaibo  2014-7-22
	 */
	public static final long toEndTime(long time) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(time);
		calendar.set(Calendar.HOUR_OF_DAY, 23);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		calendar.set(Calendar.MILLISECOND, 999);
		return calendar.getTimeInMillis();
	}

	public static final String formatDate(Date date, String formatStr, Locale locale) {
		if (formatStr == null)
			formatStr = yyyy_MM_dd_HH_mm_ss;
		SimpleDateFormat sdf = new SimpleDateFormat(formatStr, locale);
		return sdf.format(date);
	}

	public static final String formatDate(Date time, String formatStr) {
		return formatDate(time, formatStr, Locale.getDefault());
	}


	/**
	 * 向前向后推N天
	 * @param srcDate
	 * @param nDay	负数 向前 | 正数 向后
	 * @return
	 * @author zhangyingya  2014-9-10
	 */
	public static final String addDayOf(String srcDate, int nDay) {
		Date date  = strToDate(srcDate, yyyy_MM_dd);
		date = addDayOf(date, nDay);
		return getFormatDate(date, yyyy_MM_dd_HH_mm_ss_SSS);
	}

	/**
	 * 获取当前的年份
	 * @return
	 */
	public static String getThisYear() {
		Calendar now = Calendar.getInstance();

		return String.valueOf(now.get(Calendar.YEAR));
	}
	/**
	 * 获取当前的日期YYYY-MM-DD HH:mm:ss
	 * @return
	 */
	public static String getSysTime() {
		Date d = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateNowStr = sdf.format(d);
		return dateNowStr;
	}

	/**
	 * 获取当前的日期前一年的日期YYYY-MM-DD HH:mm:ss
	 * @return
	 * @throws ParseException
	 */
	public static String getLastYearTime(String dateStr) throws ParseException {
		Date d = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		d =sdf.parse(dateStr) ;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(d);
		calendar.add(Calendar.YEAR, -1);
		d = calendar.getTime();

		return sdf.format(d);
	}

	public static final String yyyy_MM_dd = "yyyy-MM-dd";
	public static final String yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";
	public static final String yyyy_MM_dd_HH_mm_ss_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";
	public static final String yyyyMMddHHmmssSSS = "yyyyMMddHHmmssSSS";
	public static final String yyyyMMdd = "yyyyMMdd";
	public static final String yyyyMM = "yyyyMM";
}
