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
package org.apache.flink.metrics.v2;


import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;

public interface MetricReporterV2 {
	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	/**
	 * Configures this reporter. Since reporters are instantiated generically and hence parameter-less,
	 * this method is the place where the reporters set their basic fields based on configuration values.
	 *
	 * <p>This method is always called first on a newly instantiated reporter.
	 *
	 * @param config A properties object that contains all parameters set for this reporter.
	 */
	void open(MetricConfig config);

	/**
	 * Closes this reporter. Should be used to close channels, streams and release resources.
	 */
	void close();

	// ------------------------------------------------------------------------
	//  adding / removing metrics
	// ------------------------------------------------------------------------

	<A, B extends Number> void notifyOfAddedMetric(MultiNumberMetric<A> metric, String metricName, MetricGroup group);

	<N extends Number> void notifyOfAddedMetric(NumberMetric<N> metric, String metricName, MetricGroup group);

	void notifyOfAddedMetric(StringMetric metric, String metricName, MetricGroup group);

	void notifyOfAddedMetric(CollectionMetric metric, String metricName, MetricGroup group);
}
