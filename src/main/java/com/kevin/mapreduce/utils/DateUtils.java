package com.kevin.mapreduce.utils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * describe  : 日期工具类
 * creat_user: zhangkai
 * creat_time: 2018/8/26 22:19
 * email     : kevin_love_it@163.com
 **/
public class DateUtils {

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
