package org.apache.spark.examples.streaming;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * Shows the most positive words in UTF8 encoded, '\n' delimited text directly
 * received the network every 5 seconds. The streaming data is joined with a
 * static RDD of the AFINN word list (http://neuro.imm.dtu.dk/wiki/AFINN)
 * 
 * Java implementation of network_wordjoinsentiments.py
 * (https://github.com/apache/spark/blob/master/examples/src/main/python/streaming/network_wordjoinsentiments.py)
 * 
 * Usage: JavaNetworkWordCount <hostname> <port> <hostname> and <port> describe
 * the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server `$
 * nc -lk 9999` and then run the example `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 * 
 * @algo
 */
public final class JavaNetworkWordCountJoinSentiments {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaNetworkWordCountJoinSentiments <hostname> <port>");
			System.exit(1);
		}

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCountJoinSentiments").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		JavaReceiverInputDStream<String> inputData = ssc.socketTextStream(args[0], Integer.parseInt(args[1]),
				StorageLevels.MEMORY_AND_DISK_SER);
		JavaDStream<String> words = inputData.flatMap(aWord -> Arrays.asList(SPACE.split(aWord)).iterator());

		// Read in the word-sentiment list and create a RDD from it
		JavaRDD<String> sentimentsInputRdd = ssc.sparkContext().textFile("src/main/resources/AFINN-111.txt");
		
		JavaDStream<Tuple2<Long, String>> happiestWords = fetchStreamingHappiestWords(words, sentimentsInputRdd);

		happiestWords.foreachRDD(sentimentWordRdd -> {
			if (!sentimentWordRdd.isEmpty()) {
				sentimentWordRdd.collect().forEach(tuple -> System.out.println(tuple._1() + " : " + tuple._2()));
				System.out.println("--");
			}
		});

		ssc.start();
		ssc.awaitTermination();
	}

	public static JavaDStream<Tuple2<Long, String>> fetchStreamingHappiestWords(JavaDStream<String> words,
			JavaRDD<String> sentimentsInputRdd) {
		JavaPairRDD<String, Integer> sentiments = sentimentsInputRdd
				.mapToPair(wordSentimentWt -> new Tuple2<>(wordSentimentWt.split("\t")[0],
						Integer.valueOf(wordSentimentWt.split("\t")[1])));

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(aWord -> new Tuple2<String, Integer>(aWord, 1))
				.reduceByKey((cnt1, cnt2) -> cnt1 + cnt2);

		JavaDStream<Tuple2<Long, String>> happiestWords = wordCounts
				.transform(aWordCountRDD -> sentiments.join(aWordCountRDD)
						.map(joinRes -> new Tuple2<String, Long>(joinRes._1(),
								new Long(joinRes._2()._1() * joinRes._2()._2())))
						.map(prodRes -> prodRes.swap()))
				.transform(sentimentWordRdd -> sentimentWordRdd.sortBy(tuple -> tuple._1(), false, 1));
		return happiestWords;
	}
}
