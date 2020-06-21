package com.ropeok.dataprocess.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    public static final int[] MONTH_DAYS = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    /**
     * 判断是否是闰年
     * @param year
     * @return
     */
    public static boolean isLeapYear(int year) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    }

    public static boolean isValidDate(int year, int month, int day) {
        if(year > 1900 && year < 2100 && month > 0 && month <= 12 && day > 0 && day <= 31) {
            return (month != 2 && MONTH_DAYS[month-1] >= day) || (month == 2 && ((isLeapYear(year) && (MONTH_DAYS[month-1]+1) >= day) || (!isLeapYear(year) && MONTH_DAYS[month-1] >= day)));
        }
        return false;
    }

    public static DateFormat DEFAULT_FORMATER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static DateFormat YYYY_MM_DD_FORMATER = new SimpleDateFormat("yyyy-MM-dd");

	//取得时间偏移量
	private static int ZONEOFFSET = Calendar.getInstance().get(Calendar.ZONE_OFFSET);
	//取得夏令时差
	private static int dstOffset = Calendar.getInstance().get(Calendar.DST_OFFSET);


    /**
     * 得到UTC时间，类型为字符串，格式为"yyyy-MM-dd HH:mm:ss"<br />
     * 如果获取失败，返回null
     * @return
     * @throws ParseException
     */
	public static String getUTCTimeStr(String gmtDate, DateFormat formater) throws ParseException {
		String utcTime = formater.format(getUTCTime(gmtDate, formater));
		return utcTime;
	}

	public static Date getUTCTime(String gmtDate, DateFormat formater) throws ParseException {
		if(StringUtils.isNotBlank(gmtDate)) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(formater.parse(gmtDate));
			// 从本地时间里扣除这些差量，即可以取得UTC时间：
			cal.add(java.util.Calendar.MILLISECOND, -(ZONEOFFSET + dstOffset));
			return cal.getTime();
		}
		return null;
	}

	public static Date getLocalTimeFromUTC(String utcTime, DateFormat formater) {
		Date utcDate = null ;
//        String localTimeStr = null ;
        try {
            utcDate = formater.parse(utcTime);
            utcDate.setTime(utcDate.getTime() + ZONEOFFSET);
//          format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
            return utcDate;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
	}

    /**
     * 将UTC时间转换为东八区时间
     * @param UTCTime
     * @return
     */
	public static String getLocalTimeFromUTCStr(String utcTime, DateFormat formater) {
		Date utcDate = getLocalTimeFromUTC(utcTime, formater);
		if (utcDate != null) {
			return formater.format(utcDate);
		}
		return null;
	}

	public static Date getLocalTimeFromUTC(Date utcTime) {
		if(utcTime != null) {
			utcTime.setTime(utcTime.getTime() + ZONEOFFSET);
			return utcTime;
		}
		return null;
	}
	public static String getLocalTimeFromUTCStr(Date utcTime, DateFormat formater) {
		if(utcTime != null) {
			Date d = getLocalTimeFromUTC(utcTime);
			return d == null ? null : formater.format(d);
		}
		return null;
	}

	public static String getDateStr(long timestamp, DateFormat formater) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(timestamp);
		return formater.format(c.getTime());
	}

}
