/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.examples.streaming.JavaNetworkWordCountJoinSentiments;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.junit.Assert;
import org.junit.Test;

import scala.Tuple2;

import static org.apache.spark.streaming.JavaAPISuite.assertOrderInvariantEquals;
import static org.junit.Assert.assertEquals;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaNetworkWordCountJoinSentimentsTest extends LocalJavaStreamingContext implements Serializable {

	public static void equalIterator(Iterator<?> a, Iterator<?> b) {
		while (a.hasNext() && b.hasNext()) {
			Assert.assertEquals(a.next(), b.next());
		}
		Assert.assertEquals(a.hasNext(), b.hasNext());
	}

	public static void equalIterable(Iterable<?> a, Iterable<?> b) {
		equalIterator(a.iterator(), b.iterator());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testStreamingJoinSentiments() {
		// Test stream input data
		List<List<String>> inputData = Arrays.asList(
				Arrays.asList("superb", "superb", "world", "thoughtful", "struggle"),
				Arrays.asList("show some", "joy", "elated", "joy"),
				Arrays.asList("struggle", "lots of", "happy", "struggle"));

		// Sentiments (sampled from resources/AFINN-111.txt)
		List<String> sentimentsInput = Arrays.asList("abandon	-2", "abandoned	-2", "abandons	-2", "elated	3",
				"elation	3", "elegant	2", "happy	3", "hard	-1", "hardier	2", "jolly	2", "jovial	2",
				"joy	3", "superb	5", "superior	2", "struck	-1", "struggle	-2", "thorny	-2", "thoughtful	2",
				"thoughtless	-2");

		JavaDStream<Tuple2<Long, String>> happiestWords = JavaNetworkWordCountJoinSentiments
				.fetchStreamingHappiestWords(JavaTestUtils.attachTestInputStream(ssc, inputData, 1),
						ssc.sparkContext().parallelize(sentimentsInput));

		JavaTestUtils.attachTestOutputStream(happiestWords);

		List<List<String>> expected = Arrays.asList(Arrays.asList("(10,superb)", "(2,thoughtful)", "(-2,struggle)"),
				Arrays.asList("(6,joy)", "(3,elated)"), Arrays.asList("(3,happy)", "(-4,struggle)"));
		
		List<List<String>> result = JavaTestUtils.runStreams(ssc, 3, 1);
		assertEquals(expected.toString(), result.toString());
	}

}
