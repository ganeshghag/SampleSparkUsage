package com.ghag.rnd.analytics.SampleSparkUsage;



import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountJava8 {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "D:\\ganeshghag\\installs\\hadoop-common-2.2.0-bin-master");
		System.out.println("word count with java 8, syntax");
		String logFile = "readme.md"; // Should be some file on
														// your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(logFile).cache();
		
		JavaRDD<String> words = textFile.flatMap(s->Arrays.asList(s.split(" ")).iterator());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

		System.out.println("output is"+counts.collectAsMap().toString());
		
		counts.saveAsTextFile("output"+new Date().getTime());
		

	}

}
