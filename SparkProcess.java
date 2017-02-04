package com.formacionhadoop;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;

public class SparkProcess {

	static String fileName = "/tmp/logsApache.txt";

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("Spark-contadores");
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(fileName);

		final Accumulator<Map<String, Integer>> contador = sc.accumulator(
				new HashMap<String, Integer>(), new MapAccumulator());

		textFile.foreach(new VoidFunction<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			// 109.169.248.247 - - [12/Dec/2015:18:25:11 +0100]
			// "GET /administrator/ HTTP/1.1" 200 4263 "-"
			// "Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0"
			// "-"\

			@Override
			public void call(String arg0) throws Exception {
				HashMap<String, Integer> cont = new HashMap<String, Integer>();

				String ip = arg0.split(" ")[0];
				cont.put(ip, 1);
				contador.add(cont);

			}
		});

		// Escritura del informe
		Writer out = null;

		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
				"/tmp/contador.txt"), "UTF-8"));

		for (Map.Entry<String, Integer> entry : contador.value().entrySet()) {

			out.write("Ip:" + entry.getKey() + "|Total:" + entry.getValue()
					+ "\n");
		}
		out.close();

	}

}
