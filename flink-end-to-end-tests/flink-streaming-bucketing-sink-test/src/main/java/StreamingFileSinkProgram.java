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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.Bucketer;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * TODO: add javadoc.
 */
public class StreamingFileSinkProgram {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String outputPath = params.getRequired("outputPath");
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(4);
		env.enableCheckpointing(4000);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

		final Path tempDirectory = new Path(outputPath);

		final StreamingFileSink<Tuple2<Integer, Integer>> sink = StreamingFileSink
			.forRowFormat(tempDirectory, (Encoder<Tuple2<Integer, Integer>>) (element, stream) -> {
				PrintStream out = new PrintStream(stream);
				out.println(element.f1);
			})
			.withBucketer(new KeyBucketer())
			.withRollingPolicy(new OnCheckpointRollingPolicy<>())
			.build();

		// generate data, shuffle, sink
		env.addSource(new Generator(10, 10, 60))
			.name("StreamingFileSinkProgram-Source")
			.keyBy(0)
			.addSink(sink)
			.name("StreamingFileSinkProgram-Sink");

		env.execute("StreamingFileSinkProgram");
	}


	/**
	 * Use first field for buckets.
	 */
	public static class KeyBucketer implements Bucketer<Tuple2<Integer, Integer>, String> {

		private static final long serialVersionUID = 987325769970523326L;

		@Override
		public String getBucketId(Tuple2<Integer, Integer> element, Context context) {
			return String.valueOf(element.f0);
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * Data-generating source function.
	 */
	public static class Generator extends RichSourceFunction<Tuple2<Integer, Integer>> implements ListCheckpointed<Tuple2<Integer, Long>> {

		private static final long serialVersionUID = -2819385275681175792L;

		private final int numKeys;
		private final int idlenessMs;
		private final int durationMs;

		private long ms = 0;

		private volatile int numRecordsEmitted = 0;
		private volatile boolean canceled = false;

		public Generator(int numKeys, int idlenessMs, int durationSeconds) {
			this.numKeys = numKeys;
			this.idlenessMs = idlenessMs;
			this.durationMs = durationSeconds * 1000;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			getRuntimeContext().getMetricGroup()
				.gauge("StreamingFileSinkTestNumRecordsEmittedBySource", () -> numRecordsEmitted);

			while (ms < durationMs) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < numKeys; i++) {
						ctx.collect(Tuple2.of(i, numRecordsEmitted));
						numRecordsEmitted++;
					}
					ms += idlenessMs;
				}
				Thread.sleep(idlenessMs);
			}

			System.out.println(numRecordsEmitted);

			while (!canceled) {
				Thread.sleep(50);
			}

		}

		@Override
		public void cancel() {
			canceled = true;
		}

		@Override
		public List<Tuple2<Integer, Long>> snapshotState(long checkpointId, long timestamp) {
			return Collections.singletonList(Tuple2.of(numRecordsEmitted, ms));
		}

		@Override
		public void restoreState(List<Tuple2<Integer, Long>> states) {
			for (Tuple2<Integer, Long> state : states) {
				numRecordsEmitted += state.f0;
				ms += state.f1;
			}
		}
	}
}
