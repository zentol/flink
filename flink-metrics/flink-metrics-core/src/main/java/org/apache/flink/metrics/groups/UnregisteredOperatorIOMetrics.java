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

package org.apache.flink.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.OperatorIOMetrics;
import org.apache.flink.metrics.OperatorMetricGroup;
import org.apache.flink.metrics.SimpleCounter;

/**
 * A special {@link OperatorIOMetrics} that contains unregistered counters.
 */
public class UnregisteredOperatorIOMetrics implements OperatorIOMetrics {
	private final Counter in = new SimpleCounter();
	private final Counter out = new SimpleCounter();

	@Override
	public Counter getNumRecordsInCounter() {
		return in;
	}

	@Override
	public Counter getNumRecordsOutCounter() {
		return out;
	}
}
