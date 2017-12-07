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

package org.apache.flink.runtime.webmonitor.handlers.savepoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.handlers.JsonFactory;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;

import akka.dispatch.OnComplete;
import com.fasterxml.jackson.core.JsonGenerator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import scala.Option;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Handler for triggering a {@link JobManagerMessages.TriggerSavepoint} message.
 */
class TriggerHandler implements RequestHandler {

	/** Current execution graphs. */
	private final ExecutionGraphHolder currentGraphs;

	/** Execution context for futures. */
	private final ExecutionContext executionContext;
	private final Object lock;
	private final Map<Long, Object> completed;
	private final Map<JobID, Long> inProgress;
	private final String triggerPathUrl;
	private final String triggerWithDirectoryPathUrl;
	private final boolean cancel;
	private final String defaultSavepointDirectory;
	private final String inProgressPathUrl;

	private AtomicLong requestCounter = new AtomicLong();

	public TriggerHandler(ExecutionGraphHolder currentGraphs, ExecutionContext executionContext, Object lock, Map<Long, Object> completed, Map<JobID, Long> inProgress, String triggerPathUrl, String triggerWithDirectoryPathUrl, boolean cancel, String defaultSavepointDirectory, String inProgressPathUrl) {
		this.currentGraphs = checkNotNull(currentGraphs);
		this.executionContext = checkNotNull(executionContext);
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
	public FullHttpResponse handleRequest(
		Map<String, String> pathParams,
		Map<String, String> queryParams,
		ActorGateway jobManager) throws Exception {

		try {
			if (jobManager != null) {
				JobID jobId = JobID.fromHexString(pathParams.get("jobid"));

				AccessExecutionGraph graph = currentGraphs.getExecutionGraph(jobId, jobManager);
				if (graph == null) {
					throw new Exception("Cannot find ExecutionGraph for job.");
				} else {
					CheckpointCoordinator coord = graph.getCheckpointCoordinator();
					if (coord == null) {
						throw new Exception("Cannot find CheckpointCoordinator for job.");
					}

					String targetDirectory = pathParams.get("targetDirectory");
					if (targetDirectory == null) {
						if (defaultSavepointDirectory == null) {
							throw new IllegalStateException("No savepoint directory configured. " +
								"You can either specify a directory when triggering this savepoint or " +
								"configure a cluster-wide default via key '" +
								ConfigConstants.SAVEPOINT_DIRECTORY_KEY + "'.");
						} else {
							targetDirectory = defaultSavepointDirectory;
						}
					}

					return handleNewRequest(jobManager, jobId, targetDirectory, coord.getCheckpointTimeout());
				}
			} else {
				throw new Exception("No connection to the leading JobManager.");
			}
		} catch (Exception e) {
			throw new Exception("Failed to cancel the job: " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private FullHttpResponse handleNewRequest(ActorGateway jobManager, final JobID jobId, String targetDirectory, long checkpointTimeout) throws IOException {
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
				Object msg = cancel
					? new JobManagerMessages.CancelJobWithSavepoint(jobId, targetDirectory)
					: new JobManagerMessages.TriggerSavepoint(jobId, Option.apply(targetDirectory));
				Future<Object> cancelFuture = jobManager
					.ask(msg, FiniteDuration.apply(checkpointTimeout, "ms"));

				cancelFuture.onComplete(new OnComplete<Object>() {
					@Override
					public void onComplete(Throwable failure, Object resp) throws Throwable {
						synchronized (lock) {
							try {
								if (resp != null) {
									if (resp.getClass() == JobManagerMessages.CancellationSuccess.class) {
										String path = ((JobManagerMessages.CancellationSuccess) resp).savepointPath();
										completed.put(requestId, path);
									} else if (resp.getClass() == JobManagerMessages.CancellationFailure.class) {
										Throwable cause = ((JobManagerMessages.CancellationFailure) resp).cause();
										completed.put(requestId, cause);
									} else {
										Throwable cause = new IllegalStateException("Unexpected CancellationResponse of type " + resp.getClass());
										completed.put(requestId, cause);
									}
								} else {
									completed.put(requestId, failure);
								}
							} finally {
								inProgress.remove(jobId);
							}
						}
					}
				}, executionContext);

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
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
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

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ConfigConstants.DEFAULT_CHARSET.name());
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		FullHttpResponse accepted = response;

		return accepted;
	}
}
