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

package org.apache.flink.runtime.rest.handler.legacy.savepoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.rest.handler.legacy.RequestHandler;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Handler for in-progress trigger savepoint operations.
 */
class InProgressHandler implements RequestHandler {

	private final Object lock;

	private final Map<Long, Object> completed;
	private final Map<JobID, Long> inProgress;
	private static final int NUM_GHOST_REQUEST_IDS = 16;
	private final ArrayDeque<Tuple2<Long, Object>> recentlyCompleted = new ArrayDeque<>(NUM_GHOST_REQUEST_IDS);

	private final String inProgressPathUrl;

	public InProgressHandler(Object lock, Map<Long, Object> completed, Map<JobID, Long> inProgress, String inProgressPathUrl) {
		this.lock = lock;
		this.completed = completed;
		this.inProgress = inProgress;

		this.inProgressPathUrl = inProgressPathUrl;
	}

	@Override
	public String[] getPaths() {
		return new String[]{inProgressPathUrl};
	}

	@Override
	@SuppressWarnings("unchecked")
	public CompletableFuture<FullHttpResponse> handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		JobID jobId = JobID.fromHexString(pathParams.get("jobid"));
		long requestId = Long.parseLong(pathParams.get("requestId"));

		return CompletableFuture.supplyAsync(
			() -> {
				try {
					synchronized (lock) {
						Object result = completed.remove(requestId);

						if (result != null) {
							// Add to recent history
							recentlyCompleted.add(new Tuple2<>(requestId, result));
							if (recentlyCompleted.size() > NUM_GHOST_REQUEST_IDS) {
								recentlyCompleted.remove();
							}

							if (result.getClass() == String.class) {
								String savepointPath = (String) result;
								return createSuccessResponse(requestId, savepointPath);
							} else {
								Throwable cause = (Throwable) result;
								return createFailureResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, requestId, cause.getMessage());
							}
						} else {
							// Check in-progress
							Long inProgressRequestId = inProgress.get(jobId);
							if (inProgressRequestId != null) {
								// Sanity check
								if (inProgressRequestId == requestId) {
									return createInProgressResponse(requestId);
								} else {
									String msg = "Request ID does not belong to JobID";
									return createFailureResponse(HttpResponseStatus.BAD_REQUEST, requestId, msg);
								}
							}

							// Check recent history
							for (Tuple2<Long, Object> recent : recentlyCompleted) {
								if (recent.f0 == requestId) {
									if (recent.f1.getClass() == String.class) {
										String savepointPath = (String) recent.f1;
										return createSuccessResponse(requestId, savepointPath);
									} else {
										Throwable cause = (Throwable) recent.f1;
										return createFailureResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, requestId, cause.getMessage());
									}
								}
							}

							return createFailureResponse(HttpResponseStatus.BAD_REQUEST, requestId, "Unknown job/request ID");
						}
					}
				} catch (Exception e) {
					throw new CompletionException(new FlinkException("Could not handle in progress request.", e));
				}
			});
	}

	private FullHttpResponse createSuccessResponse(long requestId, String savepointPath) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
		gen.writeStartObject();

		gen.writeStringField("status", "success");
		gen.writeNumberField("request-id", requestId);
		gen.writeStringField("savepoint-path", savepointPath);

		gen.writeEndObject();
		gen.close();

		String json = writer.toString();
		byte[] bytes = json.getBytes(ConfigConstants.DEFAULT_CHARSET);

		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1,
			HttpResponseStatus.CREATED,
			Unpooled.wrappedBuffer(bytes));

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		return response;
	}

	private FullHttpResponse createInProgressResponse(long requestId) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
		gen.writeStartObject();

		gen.writeStringField("status", "in-progress");
		gen.writeNumberField("request-id", requestId);

		gen.writeEndObject();
		gen.close();

		String json = writer.toString();
		byte[] bytes = json.getBytes(ConfigConstants.DEFAULT_CHARSET);

		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1,
			HttpResponseStatus.ACCEPTED,
			Unpooled.wrappedBuffer(bytes));

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		return response;
	}

	private FullHttpResponse createFailureResponse(HttpResponseStatus code, long requestId, String errMsg) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
		gen.writeStartObject();

		gen.writeStringField("status", "failed");
		gen.writeNumberField("request-id", requestId);
		gen.writeStringField("cause", errMsg);

		gen.writeEndObject();
		gen.close();

		String json = writer.toString();
		byte[] bytes = json.getBytes(ConfigConstants.DEFAULT_CHARSET);

		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1,
			code,
			Unpooled.wrappedBuffer(bytes));

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		return response;
	}
}
