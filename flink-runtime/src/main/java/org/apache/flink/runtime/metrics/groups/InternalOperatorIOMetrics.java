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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.OperatorIOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;

/**
 * This object contains shareable pre-defined IO-related metrics.
 */
public class InternalOperatorIOMetrics implements OperatorIOMetrics {

	private final InternalOperatorMetricGroup parent;

	private final Counter numRecordsIn;
	private final Counter numRecordsOut;

	private final Meter numRecordsInRate;
	private final Meter numRecordsOutRate;

	public InternalOperatorIOMetrics(InternalOperatorMetricGroup parentMetricGroup) {
		this.parent = parentMetricGroup;
		numRecordsIn = parent.counter(MetricNames.IO_NUM_RECORDS_IN);
		numRecordsOut = parent.counter(MetricNames.IO_NUM_RECORDS_OUT);
		numRecordsInRate = parent.meter(MetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn, 60));
		numRecordsOutRate = parent.meter(MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut, 60));
	}

	public Counter getNumRecordsInCounter() {
		return numRecordsIn;
	}

	public Counter getNumRecordsOutCounter() {
		return numRecordsOut;
	}

	@Override
	public Counter getNumBytesInLocalCounter() {
		return parent.parent().getIOMetrics().getNumBytesInLocalCounter();
	}

	@Override
	public Counter getNumBytesInRemoteCounter() {
		return parent.parent().getIOMetrics().getNumBytesInRemoteCounter();
	}

	@Override
	public Counter getNumBytesOutCounter() {
		return parent.parent().getIOMetrics().getNumBytesOutCounter();
	}

	public Meter getNumRecordsInRateMeter() {
		return numRecordsInRate;
	}

	public Meter getNumRecordsOutRate() {
		return numRecordsOutRate;
	}

	/**
	 * Causes the containing task to use this operators input record counter.
	 */
	public void reuseInputMetricsForTask() {
		InternalTaskIOMetrics taskIO = parent.parent().getIOMetrics();
		taskIO.reuseRecordsInputCounter(this.numRecordsIn);

	}

	/**
	 * Causes the containing task to use this operators output record counter.
	 */
	public void reuseOutputMetricsForTask() {
		InternalTaskIOMetrics taskIO = parent.parent().getIOMetrics();
		taskIO.reuseRecordsOutputCounter(this.numRecordsOut);
	}
}
