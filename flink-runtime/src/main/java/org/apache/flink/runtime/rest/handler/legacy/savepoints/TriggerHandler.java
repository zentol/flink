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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Handler for triggering a {@link JobManagerMessages.TriggerSavepoint} message.
 */
class TriggerHandler implements RequestHandler {

	/** Current execution graphs. */
	private final ExecutionGraphCache currentGraphs;

	/** Execution context for futures. */
	private final Executor executor;
	private final Object lock;
	private final Map<Long, Object> completed;
	private final Map<JobID, Long> inProgress;
	private final String triggerPathUrl;
	private final String triggerWithDirectoryPathUrl;
	private final boolean cancel;
	private final String defaultSavepointDirectory;
	private final String inProgressPathUrl;

	private AtomicLong requestCounter = new AtomicLong();

	public TriggerHandler(ExecutionGraphCache currentGraphs, Executor executor, Object lock, Map<Long, Object> completed, Map<JobID, Long> inProgress, String triggerPathUrl, String triggerWithDirectoryPathUrl, boolean cancel, String defaultSavepointDirectory, String inProgressPathUrl) {
		this.currentGraphs = checkNotNull(currentGraphs);
		this.executor = checkNotNull(executor);
		this.lock = lock;
		this.completed = completed;
		this.inProgress = inProgress;
		this.triggerPathUrl = triggerPathUrl;
		this.triggerWithDirectoryPathUrl = triggerWithDirectoryPathUrl;
		this.cancel = cancel;
		this.defaultSavepointDirectory = defaultSavepointDirectory;
		this.inProgressPathUrl = inProgressPathUrl;
	}

	@Override
	public String[] getPaths() {
		return new String[]{
			triggerPathUrl, triggerWithDirectoryPathUrl
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	public CompletableFuture<FullHttpResponse> handleRequest(
		Map<String, String> pathParams,
		Map<String, String> queryParams,
		JobManagerGateway jobManagerGateway) {

		if (jobManagerGateway != null) {
			JobID jobId = JobID.fromHexString(pathParams.get("jobid"));
			final CompletableFuture<AccessExecutionGraph> graphFuture;

			graphFuture = currentGraphs.getExecutionGraph(jobId, jobManagerGateway);

			return graphFuture.handleAsync(
				(AccessExecutionGraph graph, Throwable throwable) -> {
					if (throwable != null) {
						throw new CompletionException(new NotFoundException("Could not find ExecutionGraph with jobId " + jobId + '.'));
					} else {
						CheckpointCoordinatorConfiguration jobCheckpointingConfiguration = graph.getCheckpointCoordinatorConfiguration();
						if (jobCheckpointingConfiguration == null) {
							throw new CompletionException(new FlinkException("Cannot find checkpoint coordinator configuration for job."));
						}

						String targetDirectory = pathParams.get("targetDirectory");
						if (targetDirectory == null) {
							if (defaultSavepointDirectory == null) {
								throw new IllegalStateException("No savepoint directory configured. " +
									"You can either specify a directory when triggering this savepoint or " +
									"configure a cluster-wide default via key '" +
									CoreOptions.SAVEPOINT_DIRECTORY.key() + "'.");
							} else {
								targetDirectory = defaultSavepointDirectory;
							}
						}

						try {
							return handleNewRequest(jobManagerGateway, jobId, targetDirectory, jobCheckpointingConfiguration.getCheckpointTimeout());
						} catch (IOException e) {
							throw new CompletionException(new FlinkException("Could not cancel job with savepoint.", e));
						}
					}
				},
				executor);
		} else {
			return FutureUtils.completedExceptionally(new Exception("No connection to the leading JobManager."));
		}
	}

	@SuppressWarnings("unchecked")
	private FullHttpResponse handleNewRequest(JobManagerGateway jobManagerGateway, final JobID jobId, String targetDirectory, long checkpointTimeout) throws IOException {
		// Check whether a request exists
		final long requestId;
		final boolean isNewRequest;
		synchronized (lock) {
			if (inProgress.containsKey(jobId)) {
				requestId = inProgress.get(jobId);
				isNewRequest = false;
			} else {
				requestId = requestCounter.incrementAndGet();
				inProgress.put(jobId, requestId);
				isNewRequest = true;
			}
		}

		if (isNewRequest) {
			boolean success = false;

			try {
				// Trigger savepoint
				CompletableFuture<String> triggerFuture = cancel
					? jobManagerGateway.cancelJobWithSavepoint(jobId, targetDirectory, Time.milliseconds(checkpointTimeout))
					: jobManagerGateway.triggerSavepoint(jobId, targetDirectory, Time.milliseconds(checkpointTimeout));

				triggerFuture.whenCompleteAsync(
					(String path, Throwable throwable) -> {
						try {
							if (throwable != null) {
								completed.put(requestId, throwable);
							} else {
								completed.put(requestId, path);
							}
						} finally {
							synchronized (lock) {
								inProgress.remove(jobId);
							}
						}
					}, executor);

				success = true;
			} finally {
				synchronized (lock) {
					if (!success) {
						inProgress.remove(jobId);
					}
				}
			}
		}

		// In-progress location
		String location = inProgressPathUrl
			.replace(":jobid", jobId.toString())
			.replace(":requestId", Long.toString(requestId));

		// Accepted response
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
		gen.writeStartObject();
		gen.writeStringField("status", "accepted");
		gen.writeNumberField("request-id", requestId);
		gen.writeStringField("location", location);
		gen.writeEndObject();
		gen.close();

		String json = writer.toString();
		byte[] bytes = json.getBytes(ConfigConstants.DEFAULT_CHARSET);

		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1,
			HttpResponseStatus.ACCEPTED,
			Unpooled.wrappedBuffer(bytes));

		response.headers().set(HttpHeaders.Names.LOCATION, location);

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		FullHttpResponse accepted = response;

		return accepted;
	}
}
