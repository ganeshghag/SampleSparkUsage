package com.ghag.rnd.analytics.SampleSparkUsage;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterLinesJava8 {

	public static void main(String[] args) {

		System.out.println("filter lines with java 8 syntax");
		String logFile = "readme.md"; // Should be some file on
														// your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		JavaRDD<String> retData = logData.filter(s -> s.contains("ganesh"));
		
		System.out.println("output is"+retData.collect());

	}

}
