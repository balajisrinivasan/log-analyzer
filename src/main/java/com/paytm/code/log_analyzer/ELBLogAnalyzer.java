package com.paytm.code.log_analyzer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.collections4.ListUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class ELBLogAnalyzer implements Serializable {
	
	private static final long serialVersionUID = -3486181914260490464L;

	private ELBLogRow logRow = new ELBLogRow();
	
	private static String INPUT_FILE = "src/main/resources/2015_07_22_mktplace_shop_web_log_sample.log.gz";

	public static void main(String[] args) {
		ELBLogAnalyzer analyzer = new ELBLogAnalyzer();
		analyzer.process();
	}

	private void process() {
		SparkSession session = new SparkSession.Builder().master("local").getOrCreate();
		
		//Parse the source file, map to ELBLogRow -- session id is calculated from time & session interval.
		//for instance, if the event timestamp is 00:14:00(hh:mm:ss) and session interval is 15,
		//it's bucketed into "00:00:00-00:15:00" timeframe and given a unique id(0)
		//this serves a key to aggregate data by session
		Dataset<ELBLogRow> sourceDataset = session.read().option("delimiter", " ").option("header", false).csv(INPUT_FILE)
						.map((MapFunction<Row, ELBLogRow>) row -> logRow.parseRow(row), Encoders.bean(ELBLogRow.class));
		
		//caching the dataset or it will be recreated for each of the 4 actions below
		sourceDataset.cache();
		
		//I have avoided groupby wherever possible as it restricts non-key columns to be included in the result set
		calculateAvgSessionTime(sourceDataset);
		findLongestSession(sourceDataset);
		findUniqueHitsCount(sourceDataset);
		aggregatePageHitsByIp(sourceDataset, session);

		session.stop();
	}

	//converts to key-value, where key is the combination of session and ip
	//and value is the list of all URL visited by the IP
	//Can use set to collect urls instead of list if duplicates are to be removed
	private void aggregatePageHitsByIp(Dataset<ELBLogRow> sourceDataset, SparkSession session) {
		sourceDataset.toJavaRDD().mapToPair(row-> new Tuple2<String, ArrayList<String>>(row.getSession() + "-" + row.getIp(), new ArrayList<String>(Arrays.asList(row.getUrl()))))
		.reduceByKey((a,b)->(ArrayList<String>)ListUtils.union(a,b)).saveAsTextFile("src/main/resources/aggregateByIp.out");;
	}

	//finds count of unique hits per session and writes it to a file
	//combination of session and url is a key and this method determines the count of keys
	private void findUniqueHitsCount(Dataset<ELBLogRow> sourceDataset) {
		sourceDataset.toJavaRDD().map(row->row.getSession() + "|" + row.getUrl()).distinct()
							.mapToPair(session -> new Tuple2<String, Integer>(session.substring(0,33), 1))
							.reduceByKey((a,b)->a+b)
							.saveAsTextFile("src/main/resources/uniqueHitsCount.out");
	}

	//find the longest session in a session window and writes it to a file
	//map to pair where session is the key and value is the long session value & IP address
	private void findLongestSession(Dataset<ELBLogRow> sourceDataset) {
		sourceDataset.toJavaRDD().mapToPair(row->new Tuple2<String, Tuple2<Double, String>>(row.getSession(), new Tuple2<Double, String>(row.getSessionTime(), row.getIp())))
						.reduceByKey((a,b)-> (a._1>b._1)?a:b).saveAsTextFile("src/main/resources/longestSession.out");
	}

	//calculates the avg session time per session and writes it to a file
	private void calculateAvgSessionTime(Dataset<ELBLogRow> sourceDataset) {
		Dataset<Row> result = sourceDataset.groupBy(sourceDataset.col("session")).avg("sessionTime");
		result.write().mode(SaveMode.Overwrite).csv("src/main/resources/avgSessionTime.out");
	}

}
