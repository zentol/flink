/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.v2.ClusterClientV2;
import org.apache.flink.runtime.minicluster.v2.JobClientV2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

/**
 * TODO: add javadoc.
 */
public class Test2 extends TestLogger {

	@Test
	public void test() throws Exception {
		MiniClusterConfiguration config = new MiniClusterConfiguration();
		config.setNumJobManagers(2);
		config.setNumTaskManagerSlots(16);
		MiniCluster cluster = new MiniCluster(config);
		cluster.start();

		Thread.sleep(2000);

		JobGraph jb = getJobGraph(new String[]{"--port", "16000"});
		jb.setAllowQueuedScheduling(true);

		ClusterClientV2 client = cluster.getClient();

		JobClientV2 jc = client.submitJob(jb);

		Thread.sleep(10000);

		jc.cancelJob();
		
		jc.waitForJobTermination();

		cluster.shutdown();
	}

	private static JobGraph getJobGraph(String[] args) throws Exception {

		// the host and the port to connect to
		final String hostname;
		final int port;
		final ParameterTool params = ParameterTool.fromArgs(args);
		hostname = params.has("hostname") ? params.get("hostname") : "localhost";
		port = params.getInt("port");

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = text

			.flatMap(new FlatMapFunction<String, WordWithCount>() {
				@Override
				public void flatMap(String value, Collector<WordWithCount> out) {
					for (String word : value.split("\\s")) {
						out.collect(new WordWithCount(word, 1L));
					}
				}
			})

			.keyBy("word")
			.timeWindow(Time.seconds(5))

			.reduce(new ReduceFunction<WordWithCount>() {
				@Override
				public WordWithCount reduce(WordWithCount a, WordWithCount b) {
					return new WordWithCount(a.word, a.count + b.count);
				}
			});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);

		return StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
	}

	/**
	 * Data type for words with count.
	 */
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}
