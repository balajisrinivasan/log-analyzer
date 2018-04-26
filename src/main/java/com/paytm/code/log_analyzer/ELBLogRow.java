package com.paytm.code.log_analyzer;

import java.io.Serializable;

import org.apache.spark.sql.Row;

public class ELBLogRow implements Serializable {

	private static final long serialVersionUID = 2387325422579498416L;

	private static final int INTERVAL_IN_MINS = 15; 
	
	private String sessionId;
	//session interval for display/report
	private String session;
	private String ip;
	//duration of the session = request time + backend time + response time
	private double sessionTime;
	private String url;
	
	private String timestamp;
	private String userAgent;
	
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public double getSessionTime() {
		return sessionTime;
	}
	public void setSessionTime(double sessionTime) {
		this.sessionTime = sessionTime;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getSession() {
		return session;
	}
	public void setSession(String session) {
		this.session = session;
	} 
	
	
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	//_co, _c1 etc are column index
	//TODO: find out attributes names and replace column indexes
	//just parsing the columns needed for the assignment
	public ELBLogRow parseRow(Row row) {
		ELBLogRow logRow = new ELBLogRow();
		logRow.setSessionId(Utils.getSessionId(row.getAs("_c0"), INTERVAL_IN_MINS));
		logRow.setSession(Utils.getSession(logRow.getSessionId(), INTERVAL_IN_MINS));
		logRow.setIp(row.getAs("_c2").toString().split(":")[0]);
		//duration of the session = request time + backend time + response time
		logRow.setSessionTime(Double.valueOf(row.getAs("_c4")) + Double.valueOf(row.getAs("_c5")) + Double.valueOf(row.getAs("_c6")));
		logRow.setUrl(row.getAs("_c11").toString().split(" ")[1]);
		logRow.setTimestamp(row.getAs("_c0"));
		logRow.setUserAgent(row.getAs("_c12"));
		return logRow;
	} 
}
