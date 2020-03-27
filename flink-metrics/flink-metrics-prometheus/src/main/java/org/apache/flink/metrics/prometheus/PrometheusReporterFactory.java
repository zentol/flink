/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.util.PropertiesUtil;

import java.util.Properties;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.FILTER_LABEL_VALUE_CHARACTER;

/**
 * {@link MetricReporterFactory} for {@link PrometheusReporter}.
 */
@InterceptInstantiationViaReflection(reporterClassName = "org.apache.flink.metrics.prometheus.PrometheusReporter")
public class PrometheusReporterFactory implements MetricReporterFactory {

	private static final String ARG_PORT = "port";
	private static final String DEFAULT_PORT = "9249";

	@Override
	public PrometheusReporter createMetricReporter(Properties properties) {
		final String portsConfig = properties.getProperty(ARG_PORT, DEFAULT_PORT);

		boolean filterLabelValueCharacters = PropertiesUtil.getBoolean(properties, FILTER_LABEL_VALUE_CHARACTER.key(), FILTER_LABEL_VALUE_CHARACTER.defaultValue());
		return new PrometheusReporter(portsConfig, filterLabelValueCharacters);
	}
}
