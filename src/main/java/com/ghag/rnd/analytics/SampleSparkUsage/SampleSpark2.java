package com.ghag.rnd.analytics.SampleSparkUsage;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SampleSpark2 {

	public static void main(String[] args) {

		System.out.println("from inside find character length");
		String logFile = "readme.md"; // Should be some file on
														// your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(logFile).cache();

		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		
		System.out.println("total length="+totalLength);
		

	}

}
