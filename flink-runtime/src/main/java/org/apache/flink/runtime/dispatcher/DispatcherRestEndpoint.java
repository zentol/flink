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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.webmonitor.StatusOverviewWithVersion;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.LegacyRestHandlerAdapter;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ClusterOverviewHandler;
import org.apache.flink.runtime.rest.handler.legacy.DashboardConfigHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.legacy.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Router;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * REST endpoint for the {@link Dispatcher} component.
 */
public class DispatcherRestEndpoint extends RestServerEndpoint {

	private final GatewayRetriever<DispatcherGateway> leaderRetriever;
	private final RestHandlerConfiguration restConfiguration;
	private final Executor executor;

	public DispatcherRestEndpoint(
			RestServerEndpointConfiguration configuration,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			RestHandlerConfiguration restConfiguration,
			Executor executor) {
		super(configuration);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.restConfiguration = Preconditions.checkNotNull(restConfiguration);
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Override
	protected Collection<AbstractRestHandler<?, ?, ?, ?>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
		final Time timeout = restConfiguration.getTimeout();

		LegacyRestHandlerAdapter<DispatcherGateway, StatusOverviewWithVersion, EmptyMessageParameters> clusterOverviewHandler = new LegacyRestHandlerAdapter<>(
			restAddressFuture,
			leaderRetriever,
			timeout,
			new ClusterOverviewHeaders(),
			new ClusterOverviewHandler(
				executor,
				timeout));

		LegacyRestHandlerAdapter<DispatcherGateway, DashboardConfiguration, EmptyMessageParameters> dashboardConfigurationHandler = new LegacyRestHandlerAdapter<>(
			restAddressFuture,
			leaderRetriever,
			timeout,
			new DashboardConfigurationHeaders(),
			new DashboardConfigHandler(
				executor,
				restConfiguration.getRefreshInterval()));

		return Arrays.asList(clusterOverviewHandler, dashboardConfigurationHandler);
	}

	@Override
	protected void setupChannelHandlers(Router router, CompletableFuture<String> restAddressFuture) {
		final Time timeout = restConfiguration.getTimeout();
		final File tmpDir = restConfiguration.getTmpDir();

		Optional<StaticFileServerHandler<DispatcherGateway>> optWebContent;

		try {
			optWebContent = WebMonitorUtils.tryLoadWebContent(
				leaderRetriever,
				restAddressFuture,
				timeout,
				tmpDir);
		} catch (IOException e) {
			log.warn("Could not load web content handler.", e);
			optWebContent = Optional.empty();
		}

		optWebContent.ifPresent(
			webContentHandler -> router.GET("/:*", webContentHandler));
	}

	@Override
	public void shutdown(Time timeout) {
		super.shutdown(timeout);

		final File tmpDir = restConfiguration.getTmpDir();

		try {
			log.info("Removing cache directory {}", tmpDir);
			FileUtils.deleteDirectory(tmpDir);
		} catch (Throwable t) {
			log.warn("Error while deleting cache directory {}", tmpDir, t);
		}
	}
}
