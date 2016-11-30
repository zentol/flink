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
package org.apache.flink.metrics.influxdb;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBReporter extends AbstractReporter implements Scheduled {

	private InfluxDB connection;
	private String dbname = "metrics";
	private String fieldName = "value";

	@Override
	public String filterCharacters(String input) {
		char[] chars = null;
		final int strLen = input.length();
		int pos = 0;

		for (int i = 0; i < strLen; i++) {
			final char c = input.charAt(i);
			switch (c) {
				case '.':
					if (chars == null) {
						chars = input.toCharArray();
					}
					chars[pos++] = '_';
					break;

				default:
					if (chars != null) {
						chars[pos] = c;
					}
					pos++;
			}
		}

		return chars == null ? input : new String(chars, 0, pos);
	}

	@Override
	public void open(MetricConfig config) {
		String host = config.getString("host", "localhost");
		int port = config.getInteger("port", 8086);
		String userName = config.getString("user", "admin");
		String password = config.getString("password", "admin");

		connection = InfluxDBFactory.connect("http://" + host + ":" + port, userName, password);
		connection.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() {
	}

	@Override
	public void report() {
		long time = System.currentTimeMillis();
		Point point;
		try {
			for (Map.Entry<Gauge<?>, String> entry : gauges.entrySet()) {
				Object value = entry.getKey().getValue();
				point = value instanceof Number
					? Point.measurement(entry.getValue())
					.time(time, TimeUnit.MILLISECONDS)
					.addField(fieldName, (Number) value)
					.build()
					: Point.measurement(entry.getValue())
					.time(time, TimeUnit.MILLISECONDS)
					.addField(fieldName, value.toString())
					.build();
				connection.write(dbname, "autogen", point);
			}

			for (Map.Entry<Counter, String> entry : counters.entrySet()) {
				point = Point.measurement(entry.getValue())
					.time(time, TimeUnit.MILLISECONDS)
					.addField(fieldName, entry.getKey().getCount())
					.build();
				connection.write(dbname, "autogen", point);
			}

			for (Map.Entry<Histogram, String> entry : histograms.entrySet()) {
			}

			for (Map.Entry<Meter, String> entry : meters.entrySet()) {
				point = Point.measurement(entry.getValue())
					.time(time, TimeUnit.MILLISECONDS)
					.addField(fieldName, entry.getKey().getRate())
					.build();
				connection.write(dbname, "autogen", point);
			}
		} catch (Exception e) {
			log.warn("Exception while reporting to InfluxDB", e);
		}
	}
}
