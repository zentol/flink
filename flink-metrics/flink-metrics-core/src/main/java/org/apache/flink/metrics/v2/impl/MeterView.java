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

import org.apache.flink.metrics.View;
import org.apache.flink.metrics.v2.CounterV2;
import org.apache.flink.metrics.v2.MeterV2;

public class MeterView implements MeterV2, View {
	/** The underlying counter maintaining the count */
	private final CounterV2 counter;
	/** The time-span over which the average is calculated */
	private final int timeSpanInSeconds;
	/** Circular array containing the history of values */
	private final long[] values;
	/** The index in the array for the current time */
	private int time = 0;
	/** The last rate we computed */
	private double currentRate = 0;

	public MeterView(int timeSpanInSeconds) {
		this(new SimpleCounter(), timeSpanInSeconds);
	}

	public MeterView(CounterV2 counter, int timeSpanInSeconds) {
		this.counter = counter;
		this.timeSpanInSeconds = timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS);
		this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
	}

	@Override
	public void markEvent() {
		this.counter.inc();
	}

	@Override
	public void markEvent(long n) {
		this.counter.inc(n);
	}

	@Override
	public Double getValue() {
		return currentRate;
	}

	@Override
	public void update() {
		time = (time + 1) % values.length;
		values[time] = counter.getValue();
		currentRate = ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
	}
}
