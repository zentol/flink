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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.function.TriConsumer;

import java.util.function.Consumer;

/**
 * No-op reporter implementation.
 */
public class TestReporter implements MetricReporter, Scheduled, CharacterFilter {
	private final Consumer<MetricConfig> onOpen;
	private final Runnable onClose;
	private final TriConsumer<Metric, String, MetricGroup> onAddedMetric;
	private final TriConsumer<Metric, String, MetricGroup> onRemovedMetric;
	private final Runnable onReporter;

	TestReporter(
		final Consumer<MetricConfig> onOpen,
		final Runnable onClose,
		final TriConsumer<Metric, String, MetricGroup> onAddedMetric,
		final TriConsumer<Metric, String, MetricGroup> onRemovedMetric,
		final Runnable onReport) {

		this.onOpen = onOpen;
		this.onClose = onClose;
		this.onAddedMetric = onAddedMetric;
		this.onRemovedMetric = onRemovedMetric;
		this.onReporter = onReport;
	}

	@Override
	public void open(MetricConfig config) {
		onOpen.accept(config);
	}

	@Override
	public void close() {
		onClose.run();
	}

	@Override
	public void report() {
		onReporter.run();
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		onAddedMetric.accept(metric, metricName, group);
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		onRemovedMetric.accept(metric, metricName, group);
	}

	@Override
	public String filterCharacters(String input) {
		return input;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder.
	 */
	public static class Builder {

		private Consumer<MetricConfig> onOpen = config -> {};
		private Runnable onClose = () -> {};

		private TriConsumer<Metric, String, MetricGroup> onAddedMetric;
		private TriConsumer<Metric, String, MetricGroup> onRemovedMetric;

		private Runnable onReport = () -> {};

		Builder() {
		}

		public Builder setOnOpen(final Consumer<MetricConfig> onOpen) {
			this.onOpen = onOpen;
			return this;
		}

		public Builder setOnClose(final Runnable onClose) {
			this.onClose = onClose;
			return this;
		}

		public Builder setOnAddedMetric(final TriConsumer<Metric, String, MetricGroup> onAddedMetric) {
			this.onAddedMetric = onAddedMetric;
			return this;
		}

		public Builder setOnRemovedMetric(final TriConsumer<Metric, String, MetricGroup> onRemovedMetric) {
			this.onRemovedMetric = onRemovedMetric;
			return this;
		}

		public Builder setOnReport(final Runnable onReport) {
			this.onReport = onReport;
			return this;
		}

		public MetricReporter build() {
			return new TestReporter(onOpen, onClose, onAddedMetric, onRemovedMetric, onReport);
		}
	}
}
