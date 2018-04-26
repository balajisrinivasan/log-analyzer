package com.paytm.code.log_analyzer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Utils {
	
	
	//2015-07-22T09:00:28.019143Z
	private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
	
	//Given a time, returns a session id (combination of date and window number) for the same. 
	//For e.g time 00:14 is session id 0, 01:11 is session id 4 and so on for 15 mins interval.
	//Not an optimum solution to sessionize. The following assumptions have been made:
	//a)This will not work if 60/duration isn't an integer
	//b)and interval can only be in minutes and can't have seconds
	public static String getSessionId(String timestamp, int duration) {
		LocalDateTime dateTime = LocalDateTime.parse(timestamp, dateTimeFormatter);
		return timestamp.substring(0,10) + "-" + (dateTime.getHour() * (60/duration) + (dateTime.getMinute()/duration));
	}
	
	//Give a session window number and date, the method returns the time interval for the session window
	public static String getSession(String sessionId, int duration) {
		int sessionWindow = Integer.valueOf(sessionId.substring(11));
		String startTime = sessionId.substring(0,10) + "T00:00:00.000000Z";
		LocalDateTime dateTime = LocalDateTime.parse(startTime, dateTimeFormatter).plusMinutes(duration*sessionWindow);
		LocalDateTime endTime = dateTime.plusMinutes(duration);
		return dateTime.toString() + "-" + endTime.toString();
	}
	
}
