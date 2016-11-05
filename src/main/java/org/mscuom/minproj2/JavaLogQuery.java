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

package org.mscuom.minproj2;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple1;
import scala.Tuple2;

/**
 * Executes a roll up-style query against Apache logs.
 *
 * Usage: JavaLogQuery [logFile]
 */
public final class JavaLogQuery implements Serializable {
static Log LOG = LogFactory.getLog(JavaLogQuery.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = -7617487823754692704L;
	public static final Pattern apacheLogRegex = Pattern.compile(
			"^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");
	static Pattern traceExceptionPattern = Pattern.compile("([\\w.]+Exception)(.*)");

	private static final Properties prop = new Properties();

	/*
	 * static { try {
	 * 
	 * File file = new File(System.getProperty("pathtoconfig")); InputStream
	 * stream = new FileInputStream(file);//JavaLogQuery.class.getClassLoader().
	 * getResourceAsStream( );//"config.properties");
	 * 
	 * prop.load(stream); } catch (IOException e) { // TODO Auto-generated catch
	 * block e.printStackTrace(); } }
	 */

	public static Tuple1<String> extractKey(String line) {
		Matcher m = traceExceptionPattern.matcher(line);
		if (m.find()) {
			String ip = m.group(1);
			return new Tuple1<>(ip);
		}
		return new Tuple1<>(null);
	}

	public static StatAnalizer extractStats(String line) {
		Matcher m = traceExceptionPattern.matcher(line);
		if (m.find()) {
			return new StatAnalizer(m.group(1), m.group(0));
		} else {
			return new StatAnalizer(null, null);
		}
	}

	public static void main(String[] args) {
		LOG.info("stating application with argument -- > log file :"+args[0]+ " outpu directory :"+args[1]);
		SparkConf sparkConf = new SparkConf();

		/*
		 * if(args!=null && Boolean.valueOf(args[0])){ sparkConf= new
		 * SparkConf().setAppName("basic log query").setMaster("local"); }
		 */
		// SparkConf sparkConf =new SparkConf().setAppName("basic log
		// query").setMaster("spark://54.147.183.216:7077");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> dataSet = jsc.textFile(args[0]);// prop.getProperty("logfilepath"))
														// ;
		LOG.info("----------------------- Strat mapping ----------------------------");
		JavaPairRDD<Tuple1<String>, StatAnalizer> extracted = dataSet
				.mapToPair(new PairFunction<String, Tuple1<String>, StatAnalizer>() {
					@Override
					public Tuple2<Tuple1<String>, StatAnalizer> call(String s) {
						return new Tuple2<>(extractKey(s), extractStats(s));
					}
				});

		LOG.info("----------------------- Mapping completed----------------------------");
		
		LOG.info("Start reduceing ");
		JavaPairRDD<Tuple1<String>, StatAnalizer> counts = extracted
				.reduceByKey(new Function2<StatAnalizer, StatAnalizer, StatAnalizer>() {
					@Override
					public StatAnalizer call(StatAnalizer stats, StatAnalizer stats2) {
						return stats.merge(stats2);
					}
				});
		LOG.info("------------------------- Reduceing completed ----------------------------------");
		
		LOG.info("save to file ");
		counts.saveAsTextFile(args[1]);
		List<Tuple2<Tuple1<String>, StatAnalizer>> output = counts.collect();
		for (Tuple2<?, ?> t : output) {
			LOG.info(t._1() + "\t" + t._2());
		}
		jsc.stop();
	}
}
