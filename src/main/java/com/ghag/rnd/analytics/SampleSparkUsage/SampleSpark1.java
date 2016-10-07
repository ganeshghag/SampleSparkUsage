package com.ghag.rnd.analytics.SampleSparkUsage;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SampleSpark1 {

	public static void main(String[] args) {

		System.out.println("from inside sample spark 1");
		String logFile = "readme.md"; // Should be some file on
														// your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("this");
			}
		}).count();

		System.out.println("GG Lines with a: " + numAs + ", lines with this: " + numBs);

	}

}
