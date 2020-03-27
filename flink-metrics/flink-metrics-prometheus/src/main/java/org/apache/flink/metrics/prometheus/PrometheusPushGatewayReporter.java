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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Map;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
 */
@PublicEvolving
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterFactory")
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {

	private final PushGateway pushGateway;
	private final String jobName;
	private final boolean deleteOnShutdown;
	private final Map<String, String> groupingKey;

	public PrometheusPushGatewayReporter(
			String host,
			int port,
			String configuredJobName,
			boolean randomSuffix,
			boolean filterLabelValueCharacters,
			boolean deleteOnShutdown,
			Map<String, String> groupingKey) {
		super(filterLabelValueCharacters);

		this.deleteOnShutdown = deleteOnShutdown;
		this.groupingKey = groupingKey;

		if (host == null || host.isEmpty() || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		if (randomSuffix) {
			this.jobName = configuredJobName + new AbstractID();
		} else {
			this.jobName = configuredJobName;
		}

		pushGateway = new PushGateway(host + ':' + port);
		log.info("Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}}",
			host, port, jobName, randomSuffix, deleteOnShutdown, groupingKey);
	}

	@Override
	public void report() {
		try {
			pushGateway.push(CollectorRegistry.defaultRegistry, jobName, groupingKey);
		} catch (Exception e) {
			log.warn("Failed to push metrics to PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
		}
	}

	@Override
	public void close() {
		if (deleteOnShutdown && pushGateway != null) {
			try {
				pushGateway.delete(jobName, groupingKey);
			} catch (IOException e) {
				log.warn("Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
			}
		}
		super.close();
	}
}
