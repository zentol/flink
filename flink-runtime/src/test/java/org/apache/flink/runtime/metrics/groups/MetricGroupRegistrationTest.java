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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the registration of groups and metrics on a {@link MetricGroup}.
 */
public class MetricGroupRegistrationTest extends TestLogger {

	/**
	 * Verifies that group methods instantiate the correct metric with the given name.
	 */
	@Test
	public void testMetricInstantiation() {
		final AtomicReference<Tuple2<Metric, String>> lastAddedMetric = new AtomicReference<>();
		final MetricRegistry metricRegistry = new MetricRegistry() {
			@Override
			public char getDelimiter() {
				return 0;
			}

			@Override
			public char getDelimiter(int index) {
				return 0;
			}

			@Override
			public int getNumberReporters() {
				return 0;
			}

			@Override
			public void register(Metric metric, String metricName, AbstractMetricGroup group) {
				lastAddedMetric.set(Tuple2.of(metric, metricName));
			}

			@Override
			public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {

			}

			@Override
			public ScopeFormats getScopeFormats() {
				return null;
			}

			@Nullable
			@Override
			public String getMetricQueryServicePath() {
				return null;
			}
		};

		final MetricGroup group = new TaskManagerMetricGroup(metricRegistry, "host", "id");

		final Counter counter = group.counter("counter");
		assertEquals(counter, lastAddedMetric.get().f0);
		assertEquals("counter", lastAddedMetric.get().f1);

		final Meter meter = group.meter("meter", new TestMeter());
		assertEquals(meter, lastAddedMetric.get().f0);
		assertEquals("meter", lastAddedMetric.get().f1);

		final Gauge<Object> gauge = group.gauge("gauge", () -> null);
		assertEquals(gauge, lastAddedMetric.get().f0);
		assertEquals("gauge", lastAddedMetric.get().f1);

		final Histogram histogram = group.histogram("histogram", new TestHistogram());
		assertEquals(histogram, lastAddedMetric.get().f0);
		assertEquals("histogram", lastAddedMetric.get().f1);
	}

	/**
	 * Verifies that when attempting to create a group with the name of an existing one the existing one will be returned instead.
	 */
	@Test
	public void testDuplicateGroupName() {
		MetricGroup root = new TaskManagerMetricGroup(NoOpMetricRegistry.INSTANCE, "host", "id");

		MetricGroup group1 = root.addGroup("group");
		MetricGroup group2 = root.addGroup("group");
		MetricGroup group3 = root.addGroup("group");
		Assert.assertTrue(group1 == group2 && group2 == group3);
	}
}
