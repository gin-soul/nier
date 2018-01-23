package com.nier.utils;



import java.util.Calendar;
import java.util.Date;

public class DateUtils {
	private static Calendar calendar = Calendar.getInstance();
	/**
	 * 获得当前时间距离给定天数零点的毫秒时间
	 * @param amount
	 * @return
	 */
	public static Long getDelayTime(int amount){
		//将时分秒设置成0
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		//3 设置指定天数
		calendar.add(Calendar.DATE, amount);
		//4 计算当前时间距离设置日期零点的延迟时间
		return calendar.getTimeInMillis() - newDate.getTime();
	}
	
	
	/**
	 * 当前时间距离明天零点的毫秒时间
	 * @return
	 */
	public static Long getDelayTime(){
		return getDelayTime(1);
	}
	
	
	/**
	 * 获得一天的毫秒值
	 * @return
	 */
	public static Long getOneDay(){
		return 24 * 60 * 60 * 1000L;
	}
	
	/**
	 * 获得几月(两位)
	 * @return
	 */
	public static String getCurrentMonth(){
		//1 设置当前时间
		int m = calendar.get(Calendar.MONTH) + 1;
		if(m < 10){
			return "0" + m;
		}
		return "" + m;
	}
	/**
	 * 获得几号(两位)
	 * @return
	 */
	public static String getCurrentDay(){
		int d = calendar.get(Calendar.DATE);
		if(d < 10){
			return "0" + d;
		}
		return "" + d;
	}
	/**
	 * 将时间转换成时间戳
	 * @param time
	 * @return
	 * @throws ParseException 
	 */
	public static String getTimeStamp(String time) throws ParseException{
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		//将时间字符串转换成Date类型
		Date date = dateFormat.parse(time);
		//Date类型又能转换成时间戳
		long time2 = date.getTime();
		return time2+"";
	}
	/**
	 * 获取当前时间
	 * @return
	 */
	public static String getCurrentTime(){
		Date date = new Date();
		//将date转换成字符串类型
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return dateFormat.format(date);
	}
	/**
	 * 将时间戳转换成时间字符串
	 * @return
	 */
	public static String getTimeString(String timeStamp){
		//1.将时间戳字符串转换成long类型
		long stamp = Long.parseLong(timeStamp);
		//2.将时间戳转换成Date类型
		Date date = new Date(stamp);
		//3.将date转换成字符串类型
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		return format.format(date);
	}
}
