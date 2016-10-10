package com.ghag.rnd.analytics.SampleSparkUsage;



import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {

		System.out.println("from inside sample spark 1");
		String logFile = "readme.md"; // Should be some file on
														// your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(logFile).cache();

		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			  public Iterator<String> call(String s) { return Arrays.asList(s.split(" ")).iterator(); }
			});
			JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
			});
			JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			  public Integer call(Integer a, Integer b) { return a + b; }
			});
			

			System.out.println("output is"+counts.collectAsMap().toString());
		

	}

}
