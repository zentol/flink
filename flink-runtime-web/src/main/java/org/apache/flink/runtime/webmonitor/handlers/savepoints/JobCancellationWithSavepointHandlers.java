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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJobWithSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler for {@link CancelJobWithSavepoint} messages.
 */
public class JobCancellationWithSavepointHandlers {

	private static final String CANCEL_WITH_SAVEPOINT_REST_PATH = "/jobs/:jobid/cancel-with-savepoint";
	private static final String CANCEL_WITH_SAVEPOINT_DIRECTORY_REST_PATH = "/jobs/:jobid/cancel-with-savepoint/target-directory/:targetDirectory";

	/** URL for in-progress cancellations. */
	private static final String CANCELLATION_IN_PROGRESS_REST_PATH = "/jobs/:jobid/cancel-with-savepoint/in-progress/:requestId";

	/** Handler for trigger requests. */
	private final TriggerHandler triggerHandler;

	/** Handler for in-progress requests. */
	private final InProgressHandler inProgressHandler;

	public JobCancellationWithSavepointHandlers(
			ExecutionGraphHolder currentGraphs,
			ExecutionContext executionContext,
			@Nullable String defaultSavepointDirectory) {

		/* In-Progress requests. */
		Map<JobID, Long> inProgress = new HashMap<>();
		/* Succeeded/failed request. Either String or Throwable. */
		Map<Long, Object> completed = new HashMap<>();
		/* Shared lock between Trigger and In-Progress handlers. */
		Object lock = new Object();

		this.triggerHandler = new TriggerHandler(
			currentGraphs,
			executionContext,
			lock,
			completed,
			inProgress,
			CANCEL_WITH_SAVEPOINT_REST_PATH,
			CANCEL_WITH_SAVEPOINT_DIRECTORY_REST_PATH,
			true, defaultSavepointDirectory,
			CANCELLATION_IN_PROGRESS_REST_PATH);
		this.inProgressHandler = new InProgressHandler(lock, completed, inProgress, CANCELLATION_IN_PROGRESS_REST_PATH);
	}

	public RequestHandler getTriggerHandler() {
		return triggerHandler;
	}

	public RequestHandler getInProgressHandler() {
		return inProgressHandler;
	}
}
