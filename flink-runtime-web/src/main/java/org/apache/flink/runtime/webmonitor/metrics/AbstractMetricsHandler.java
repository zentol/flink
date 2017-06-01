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

package org.apache.flink.runtime.webmonitor.metrics;

import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import org.apache.flink.runtime.webmonitor.RuntimeMonitorHandlerBase;
import org.apache.flink.runtime.webmonitor.handlers.JsonFactory;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonGenerator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.handler.codec.http.router.Routed;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future$;
import scala.concurrent.duration.FiniteDuration;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.runtime.webmonitor.files.StaticFileServerHandler.sendError;

/**
 * Abstract request handler that returns a list of all available metrics or the values for a set of metrics.
 *
 * <p>If the query parameters do not contain a "get" parameter the list of all metrics is returned.
 * {@code [ { "id" : "X" } ] }
 *
 * <p>If the query parameters do contain a "get" parameter a comma-separate list of metric names is expected as a value.
 * {@code /get?X,Y}
 * The handler will then return a list containing the values of the requested metrics.
 * {@code [ { "id" : "X", "value" : "S" }, { "id" : "Y", "value" : "T" } ] }
 */
public abstract class AbstractMetricsHandler extends RuntimeMonitorHandlerBase {
	private final MetricFetcher fetcher;
	private final ExecutionContextExecutor executor;

	public AbstractMetricsHandler(
		JobManagerRetriever retriever,
		scala.concurrent.Future<String> localJobManagerAddressPromise,
		FiniteDuration timeout,
		boolean httpsEnabled,
		ExecutionContextExecutor executor,
		MetricFetcher fetcher) {
		super(retriever, localJobManagerAddressPromise, timeout, httpsEnabled);
		this.fetcher = Preconditions.checkNotNull(fetcher);
		this.executor = executor;
	}

	@Override
	protected void respondAsLeader(final ChannelHandlerContext ctx, final Routed routed, final ActorGateway jobManager) {
		final Map<String, String> pathParams = routed.pathParams();

		final Map<String, String> queryParams = new HashMap<>();
		for (String key : routed.queryParams().keySet()) {
			queryParams.put(key, routed.queryParam(key));
		}

		try {
			scala.concurrent.Future<String> start = Future$.MODULE$.successful(queryParams.get("get"));
			Future<String> request = new FlinkFuture<>(start);
			Future<String> response = request.thenApplyAsync(new ApplyFunction<String, String>() {
				@Override
				public String apply(String requestedMetricsList) {
					try {
						fetcher.update();

						return requestedMetricsList != null
							? getMetricsValues(pathParams, requestedMetricsList)
							: getAvailableMetricsList(pathParams);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}, executor);

			response.thenAcceptAsync(new AcceptFunction<String>() {
				@Override
				public void accept(String value) {
					Charset encoding = Charset.forName("UTF-8");
					byte[] bytes = value.getBytes(encoding);

					DefaultFullHttpResponse response = new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));

					response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
					response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
					response.headers().set(HttpHeaders.Names.CONTENT_ENCODING, encoding.name());

					KeepAliveWrite.flush(ctx, routed.request(), response);
				}
			}, executor);
		} catch (Exception e) {
			sendError(ctx, INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Returns a Map containing the metrics belonging to the entity pointed to by the path parameters.
	 *
	 * @param pathParams REST path parameters
	 * @param metrics MetricStore containing all metrics
	 * @return Map containing metrics, or null if no metric exists
	 */
	protected abstract Map<String, String> getMapFor(Map<String, String> pathParams, MetricStore metrics);

	private String getMetricsValues(Map<String, String> pathParams, String requestedMetricsList) throws IOException {
		if (requestedMetricsList.isEmpty()) {
			/*
			 * The WebInterface doesn't check whether the list of available metrics was empty. This can lead to a
			 * request for which the "get" parameter is an empty string.
			 */
			return "";
		}
		MetricStore metricStore = fetcher.getMetricStore();
		synchronized (metricStore) {
			Map<String, String> metrics = getMapFor(pathParams, metricStore);
			if (metrics == null) {
				return "";
			}
			String[] requestedMetrics = requestedMetricsList.split(",");

			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

			gen.writeStartArray();
			for (String requestedMetric : requestedMetrics) {
				Object metricValue = metrics.get(requestedMetric);
				if (metricValue != null) {
					gen.writeStartObject();
					gen.writeStringField("id", requestedMetric);
					gen.writeStringField("value", metricValue.toString());
					gen.writeEndObject();
				}
			}
			gen.writeEndArray();

			gen.close();
			return writer.toString();
		}
	}

	private String getAvailableMetricsList(Map<String, String> pathParams) throws IOException {
		MetricStore metricStore = fetcher.getMetricStore();
		synchronized (metricStore) {
			Map<String, String> metrics = getMapFor(pathParams, metricStore);
			if (metrics == null) {
				return "";
			}
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

			gen.writeStartArray();
			for (String m : metrics.keySet()) {
				gen.writeStartObject();
				gen.writeStringField("id", m);
				gen.writeEndObject();
			}
			gen.writeEndArray();

			gen.close();
			return writer.toString();
		}
	}
}
