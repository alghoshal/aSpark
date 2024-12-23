package org.apache.spark;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.io.Files;

/**
 * JavaSparkAPI tests
 */
public class JavaSparkAPITestSuite {
	private transient JavaSparkContext sc;
	private transient File tempDir;

	@Before
	public void setUp() {
		sc = new JavaSparkContext("local", "JavaAPISuite");
		tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
	}

	@After
	public void tearDown() {
		sc.stop();
		sc = null;
	}

	@Test
	public void parallelizeNumSlices() {
		List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> rdd = sc.parallelize(ints);
		assertEquals(1, rdd.getNumPartitions());
		assertEquals("[10]",
				rdd.mapPartitionsWithIndex((anInt, itr) -> Arrays.asList(Iterators.size(itr)).iterator(), true)
						.collect().toString());

		int numPartitions = 3;
		JavaRDD<Integer> rddSliced = sc.parallelize(ints, numPartitions);
		assertEquals(numPartitions, rddSliced.getNumPartitions());
		
		// Just to print
		rddSliced.foreachPartition(itr -> System.out.print(Iterators.size(itr)+", "));

		assertEquals("[3, 3, 4]",
				rddSliced.mapPartitions(itr -> Collections.singletonList(Iterators.size(itr)).iterator()).collect().toString());

		/*
		 * assertEquals("[3, 3, 4]", rddSliced.mapPartitionsWithIndex((anInt, itr) ->
		 * Arrays.asList(Iterators.size(itr)).iterator(), true) .collect().toString());
		 */
	}

}
