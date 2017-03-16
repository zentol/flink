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

import org.apache.flink.metrics.v2.HistogramStatistics;
import org.apache.flink.metrics.v2.HistogramV2;
import org.apache.flink.metrics.v2.SubNumberMetric;

import java.util.ArrayList;
import java.util.Collection;

public class HistogramImpl implements HistogramV2 {

	private static final Collection<SubNumberMetric<HistogramStatistics, ? extends Number>> subMetrics;

	static {
		subMetrics = new ArrayList<>();
		subMetrics.add(new P99());
		subMetrics.add(new Max());
	}

	@Override
	public HistogramStatistics getContainer() {
		return new HistogramStatisticsImpl();
	}

	@Override
	public Collection<SubNumberMetric<HistogramStatistics, ? extends Number>> getSubNumberMetrics() {
		return subMetrics;
	}

	@Override
	public void update(long value) {
	}

	private static class P99 implements SubNumberMetric<HistogramStatistics, Double> {
		@Override
		public Double getValue(HistogramStatistics container) {
			return container.getQuantile(0.99);
		}

		@Override
		public String getName() {
			return "p99";
		}
	}

	private static class Max implements SubNumberMetric<HistogramStatistics, Long> {
		@Override
		public Long getValue(HistogramStatistics container) {
			return container.getMax();
		}

		@Override
		public String getName() {
			return "max";
		}
	}

	private static class HistogramStatisticsImpl extends HistogramStatistics {

		@Override
		public double getQuantile(double quantile) {
			return 0;
		}

		@Override
		public long[] getValues() {
			return new long[0];
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public double getMean() {
			return 0;
		}

		@Override
		public double getStdDev() {
			return 0;
		}

		@Override
		public long getMax() {
			return 0;
		}

		@Override
		public long getMin() {
			return 0;
		}
	}
}
