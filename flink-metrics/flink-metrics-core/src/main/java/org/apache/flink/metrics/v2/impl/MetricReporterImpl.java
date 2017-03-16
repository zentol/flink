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
package org.apache.flink.metrics.v2.impl;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.v2.CollectionMetric;
import org.apache.flink.metrics.v2.MetricReporterV2;
import org.apache.flink.metrics.v2.MultiNumberMetric;
import org.apache.flink.metrics.v2.NumberMetric;
import org.apache.flink.metrics.v2.StringMetric;
import org.apache.flink.metrics.v2.SubNumberMetric;

import java.util.Collection;

public class MetricReporterImpl implements MetricReporterV2 {

	public static void main(String[] args) {
		MetricReporterImpl rep = new MetricReporterImpl();

		HistogramImpl his = new HistogramImpl();

		rep.notifyOfAddedMetric(his, "his", null);
	}

	@Override
	public void open(MetricConfig config) {

	}

	@Override
	public void close() {

	}

	@Override
	public <A, N extends Number> void notifyOfAddedMetric(MultiNumberMetric<A> metric, String metricName, MetricGroup group) {
		Collection<SubNumberMetric<A, N>> subMetrics = metric.getSubNumberMetrics();
		A container = metric.getContainer();
		for (SubNumberMetric<A, N> subMetric : subMetrics) {
			N value = subMetric.getValue(container);
			String name = subMetric.getName();
			System.out.println(value);
		}
	}

	@Override
	public <N extends Number> void notifyOfAddedMetric(NumberMetric<N> metric, String metricName, MetricGroup group) {
	}

	@Override
	public void notifyOfAddedMetric(StringMetric metric, String metricName, MetricGroup group) {
	}

	@Override
	public void notifyOfAddedMetric(CollectionMetric metric, String metricName, MetricGroup group) {
	}
}
