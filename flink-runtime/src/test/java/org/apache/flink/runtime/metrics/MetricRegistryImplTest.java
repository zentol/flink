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

package org.apache.flink.runtime.metrics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorNotFound;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link MetricRegistryImpl}.
 */
public class MetricRegistryImplTest extends TestLogger {

	@Test
	public void testIsShutdown() throws Exception {
		final MetricRegistryImpl metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		try {
			assertFalse(metricRegistry.isShutdown());

			metricRegistry.shutdown().get();

			assertTrue(metricRegistry.isShutdown());
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	/**
	 * Verifies that multiple reporters are instantiated correctly.
	 */
	@Test
	public void testMultipleReporterInstantiation() throws Exception {
		final AtomicBoolean rep1Opened = new AtomicBoolean();
		final AtomicBoolean rep2Opened = new AtomicBoolean();
		final AtomicBoolean rep3Opened = new AtomicBoolean();

		final MetricReporter rep1 = TestReporter.builder().setOnOpen(config -> rep1Opened.set(true)).build();
		final MetricReporter rep2 = TestReporter.builder().setOnOpen(config -> rep2Opened.set(true)).build();
		final MetricReporter rep3 = TestReporter.builder().setOnOpen(config -> rep3Opened.set(true)).build();

		final MetricRegistryImpl metricRegistry = MetricRegistryTestUtils.createRegistry(
			new MetricRegistryConfiguration.ReporterSetup("test1", new MetricConfig(), () -> rep1),
			new MetricRegistryConfiguration.ReporterSetup("test2", new MetricConfig(), () -> rep2),
			new MetricRegistryConfiguration.ReporterSetup("test3", new MetricConfig(), () -> rep3));

		try {
			assertEquals(3, metricRegistry.getReporters().size());

			assertTrue(rep1Opened.get());
			assertTrue(rep2Opened.get());
			assertTrue(rep3Opened.get());
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	/**
	 * Verifies that reporters implementing the Scheduled interface are regularly called to report the metrics.
	 */
	@Test
	public void testReporterScheduling() throws Exception {
		final AtomicInteger reportCounter = new AtomicInteger();
		final MetricReporter rep = TestReporter.builder().setOnReport(reportCounter::incrementAndGet).build();
		final MetricConfig config = new MetricConfig();
		config.setProperty(ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, "50 MILLISECONDS");

		final MetricRegistryImpl registry = MetricRegistryTestUtils.createRegistry(new MetricRegistryConfiguration.ReporterSetup("test", config, () -> rep));
		try {
			long start = System.currentTimeMillis();

			// only start counting from now on
			reportCounter.set(0);

			for (int x = 0; x < 10; x++) {
				Thread.sleep(100);
				final int reportCount = reportCounter.get();
				final long curT = System.currentTimeMillis();
				/**
				 * Within a given time-frame T only T/500 reports may be triggered due to the interval between reports.
				 * This value however does not not take the first triggered report into account (=> +1).
				 * Furthermore we have to account for the mis-alignment between reports being triggered and our time
				 * measurement (=> +1); for T=200 a total of 4-6 reports may have been
				 * triggered depending on whether the end of the interval for the first reports ends before
				 * or after T=50.
				 */
				final long maxAllowedReports = (curT - start) / 50 + 2;
				assertTrue("Too many reports were triggered.", maxAllowedReports >= reportCount);
			}
			assertTrue("No report was triggered.", reportCounter.get() > 0);
		} finally {
			registry.shutdown().get();
		}
	}

	/**
	 * Verifies that reporters are notified of added/removed metrics.
	 */
	@Test
	public void testReporterNotifications() throws Exception {
		final AtomicReference<Tuple2<Metric, String>> lastAddedMetric1 = new AtomicReference<>();
		final AtomicReference<Tuple2<Metric, String>> lastAddedMetric2 = new AtomicReference<>();
		final AtomicReference<Tuple2<Metric, String>> lastRemovedMetric1 = new AtomicReference<>();
		final AtomicReference<Tuple2<Metric, String>> lastRemovedMetric2 = new AtomicReference<>();

		final MetricReporter rep1 = TestReporter.builder()
			.setOnAddedMetric((metric, name, group) -> lastAddedMetric1.set(Tuple2.of(metric, name)))
			.setOnRemovedMetric((metric, name, group) -> lastRemovedMetric1.set(Tuple2.of(metric, name)))
			.build();
		final MetricReporter rep2 = TestReporter.builder()
			.setOnAddedMetric((metric, name, group) -> lastAddedMetric2.set(Tuple2.of(metric, name)))
			.setOnRemovedMetric((metric, name, group) -> lastRemovedMetric2.set(Tuple2.of(metric, name)))
			.build();

		final MetricRegistryImpl metricRegistry = MetricRegistryTestUtils.createRegistry(
			new MetricRegistryConfiguration.ReporterSetup("test1", new MetricConfig(), () -> rep1),
			new MetricRegistryConfiguration.ReporterSetup("test2", new MetricConfig(), () -> rep2));

		final String metricName = "counter";
		final Metric metric = new SimpleCounter();
		final TaskManagerMetricGroup group = new TaskManagerMetricGroup(metricRegistry, "host", "id");

		try {
			metricRegistry.register(metric, metricName, group);

			assertEquals(metric, lastAddedMetric1.get().f0);
			assertEquals(metricName, lastAddedMetric1.get().f1);

			assertEquals(metric, lastAddedMetric2.get().f0);
			assertEquals(metricName, lastAddedMetric2.get().f1);

			metricRegistry.unregister(metric, metricName, group);

			assertEquals(metric, lastRemovedMetric1.get().f0);
			assertEquals(metricName, lastRemovedMetric1.get().f1);

			assertEquals(metric, lastRemovedMetric2.get().f0);
			assertEquals(metricName, lastRemovedMetric2.get().f1);
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	@Test
	public void testConfigurableDelimiter() throws Exception {
		final Configuration config = new Configuration();
		config.setString(MetricOptions.SCOPE_DELIMITER, "_");
		config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D.E");

		final MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

		try {
			final TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(registry, "host", "id");
			assertEquals("A_B_C_D_E_name", tmGroup.getMetricIdentifier("name"));
		} finally {
			registry.shutdown().get();
		}
	}

	@Test
	public void testConfigurableDelimiterForReporters() throws Exception {
		final MetricConfig config1 = new MetricConfig();
		config1.setProperty(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "_");

		final MetricConfig config2 = new MetricConfig();
		config1.setProperty(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "AA");

		final MetricRegistryImpl metricRegistry = MetricRegistryTestUtils.createRegistry(
			new MetricRegistryConfiguration.ReporterSetup("test1", config1, () -> TestReporter.builder().build()),
			new MetricRegistryConfiguration.ReporterSetup("test2", config2, () -> TestReporter.builder().build()));

		try {
			assertEquals(MetricOptions.SCOPE_DELIMITER.defaultValue(), metricRegistry.getDelimiter());
			assertEquals('_', metricRegistry.getDelimiter(0));
			// invalid delimiter, revert to global
			assertEquals(MetricOptions.SCOPE_DELIMITER.defaultValue(), metricRegistry.getDelimiter(1));
			// out-of bounds, revert
			assertEquals(MetricOptions.SCOPE_DELIMITER.defaultValue(), metricRegistry.getDelimiter(2));
			assertEquals(MetricOptions.SCOPE_DELIMITER.defaultValue(), metricRegistry.getDelimiter(-1));
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	@Test
	public void testConfigurableDelimiterForReportersInGroup() throws Exception {
		// valid configured delimiter
		final Tuple3<MetricReporter, AtomicReference<String>, MetricConfig> rep1 = setupMetricIdentifierExposingReporter(Optional.of("_"));

		// invalid configured delimiter
		final Tuple3<MetricReporter, AtomicReference<String>, MetricConfig> rep2 = setupMetricIdentifierExposingReporter(Optional.of("AA"));

		// use default delimiter
		final Tuple3<MetricReporter, AtomicReference<String>, MetricConfig> rep3 = setupMetricIdentifierExposingReporter(Optional.empty());

		final Configuration config = new Configuration();
		config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B");

		final MetricRegistryImpl metricRegistry = MetricRegistryTestUtils.createRegistry(
			ScopeFormats.fromConfig(config),
			new MetricRegistryConfiguration.ReporterSetup("test1", rep1.f2, () -> rep1.f0),
			new MetricRegistryConfiguration.ReporterSetup("test2", rep2.f2, () -> rep2.f0),
			new MetricRegistryConfiguration.ReporterSetup("test3", rep2.f2, () -> rep3.f0));

		final String metricName = "counter";
		final Metric metric = new SimpleCounter();
		final TaskManagerMetricGroup group = new TaskManagerMetricGroup(metricRegistry, "host", "id");

		try {
			metricRegistry.register(metric, metricName, group);

			assertEquals("A_B_counter", rep1.f1.get());
			assertEquals("A.B.counter", rep2.f1.get());
			assertEquals("A.B.counter", rep3.f1.get());

			metricRegistry.unregister(metric, metricName, group);
			assertEquals("A_B_counter", rep1.f1.get());
			assertEquals("A.B.counter", rep2.f1.get());
			assertEquals("A.B.counter", rep3.f1.get());
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	private static Tuple3<MetricReporter, AtomicReference<String>, MetricConfig> setupMetricIdentifierExposingReporter(final Optional<String> delimiter) {
		final MetricConfig config = new MetricConfig();
		if (delimiter.isPresent()) {
			config.setProperty(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "_");
		}

		final AtomicReference<String> lastMetricIdentifier = new AtomicReference<>();
		final MetricReporter rep = TestReporter.builder().setOnAddedMetric((metric, name, group) -> lastMetricIdentifier.set(group.getMetricIdentifier(name))).build();

		return Tuple3.of(rep, lastMetricIdentifier, config);
	}

	/**
	 * Tests that the query actor will be stopped when the MetricRegistry is shut down.
	 */
	@Test
	public void testQueryActorShutdown() throws Exception {
		final FiniteDuration timeout = new FiniteDuration(10L, TimeUnit.SECONDS);

		final MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		try {
			final ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
			try {
				registry.startQueryService(actorSystem, null);

				final ActorRef queryServiceActor = registry.getQueryService();

				registry.shutdown().get();

				try {
					Await.result(actorSystem.actorSelection(queryServiceActor.path()).resolveOne(timeout), timeout);

					fail("The query actor should be terminated resulting in a ActorNotFound exception.");
				} catch (ActorNotFound e) {
					// we expect the query actor to be shut down
				}
			} finally {
				actorSystem.shutdown();
			}
		} finally {
			registry.shutdown().get();
		}
	}

	@Test
	public void testExceptionIsolation() throws Exception {
		final AtomicReference<Tuple2<Metric, String>> lastAddedMetric1 = new AtomicReference<>();
		final AtomicReference<Tuple2<Metric, String>> lastRemovedMetric1 = new AtomicReference<>();

		final MetricReporter rep1 = TestReporter.builder()
			.setOnAddedMetric((metric, name, group) -> lastAddedMetric1.set(Tuple2.of(metric, name)))
			.setOnRemovedMetric((metric, name, group) -> lastRemovedMetric1.set(Tuple2.of(metric, name)))
			.build();

		final MetricReporter rep2 = TestReporter.builder()
			.setOnAddedMetric((metric, name, group) -> {
				throw new RuntimeException();
			})
			.setOnRemovedMetric((metric, name, group) -> {
				throw new RuntimeException();
			})
			.build();

		final MetricRegistryImpl metricRegistry = MetricRegistryTestUtils.createRegistry(
			new MetricRegistryConfiguration.ReporterSetup("test1", new MetricConfig(), () -> rep1),
			new MetricRegistryConfiguration.ReporterSetup("test2", new MetricConfig(), () -> rep2));

		final String metricName = "rootCounter";
		final Metric metric = new SimpleCounter();
		final TaskManagerMetricGroup group = new TaskManagerMetricGroup(metricRegistry, "host", "id");

		try {
			metricRegistry.register(metric, metricName, group);

			assertEquals(metric, lastAddedMetric1.get().f0);
			assertEquals(metricName, lastAddedMetric1.get().f1);

			metricRegistry.unregister(metric, metricName, group);

			assertEquals(metric, lastRemovedMetric1.get().f0);
			assertEquals(metricName, lastRemovedMetric1.get().f1);
		} finally {
			metricRegistry.shutdown().get();
		}
	}
}
