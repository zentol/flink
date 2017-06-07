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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJobWithSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.types.Either;

import javax.annotation.Nullable;

import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;

/**
 * Request handler for {@link CancelJobWithSavepoint} messages.
 */
public class JobCancellationWithSavepointHandlers extends AbstractSavepointHandlers {

	/** URLs for in-progress cancellations. */
	private static final String CANCEL_WITH_SAVEPOINT_REST_PATH = "/jobs/:jobid/cancel-with-savepoint";
	private static final String CANCEL_WITH_SAVEPOINT_DIRECTORY_REST_PATH = "/jobs/:jobid/cancel-with-savepoint/target-directory/:targetDirectory";
	private static final String CANCELLATION_IN_PROGRESS_REST_PATH = "/jobs/:jobid/cancel-with-savepoint/in-progress/:requestId";

	/** Handler for trigger requests. */
	private final TriggerHandler triggerHandler;

	/** Handler for in-progress requests. */
	private final InProgressHandler inProgressHandler;

	public JobCancellationWithSavepointHandlers(
			ExecutionGraphHolder currentGraphs,
			ExecutionContext executionContext) {
		this(currentGraphs, executionContext, null);
	}

	public JobCancellationWithSavepointHandlers(
			ExecutionGraphHolder currentGraphs,
			ExecutionContext executionContext,
			@Nullable String defaultSavepointDirectory) {
		super(defaultSavepointDirectory);

		this.triggerHandler = new TriggerHandler(currentGraphs, executionContext);
		this.inProgressHandler = new InProgressHandler();
	}

	public TriggerHandler getTriggerHandler() {
		return triggerHandler;
	}

	public InProgressHandler getInProgressHandler() {
		return inProgressHandler;
	}

	// ------------------------------------------------------------------------
	// New requests
	// ------------------------------------------------------------------------

	/**
	 * Handler for triggering a {@link CancelJobWithSavepoint} message.
	 */
	class TriggerHandler extends AbstractTriggerHandler {

		TriggerHandler(ExecutionGraphHolder currentGraphs, ExecutionContext executionContext) {
			super(currentGraphs, executionContext);
		}

		@Override
		protected scala.concurrent.Future<Object> sendSavepointRequest(JobID jobId, String targetDirectory, ActorGateway jobManager, long checkpointTimeout) {
			// Trigger cancellation
			Object msg = new CancelJobWithSavepoint(jobId, targetDirectory);
			return jobManager
				.ask(msg, FiniteDuration.apply(checkpointTimeout, "ms"));
		}

		@Override
		protected Either<String, Throwable> validateResponse(Object resp) {
			if (resp.getClass() == CancellationSuccess.class) {
				String path = ((CancellationSuccess) resp).savepointPath();
				return Either.Left(path);
			} else if (resp.getClass() == CancellationFailure.class) {
				Throwable cause = ((CancellationFailure) resp).cause();
				return Either.Right(cause);
			} else {
				Throwable cause = new IllegalStateException("Unexpected CancellationResponse of type " + resp.getClass());
				return Either.Right(cause);
			}
		}

		@Override
		public String[] getPaths() {
			return new String[]{CANCEL_WITH_SAVEPOINT_REST_PATH, CANCEL_WITH_SAVEPOINT_DIRECTORY_REST_PATH};
		}
	}

	// ------------------------------------------------------------------------
	// In-progress requests
	// ------------------------------------------------------------------------

	/**
	 * Handler for in-progress cancel with savepoint operations.
	 */
	class InProgressHandler extends AbstractInProgressHandler {

		@Override
		public String[] getPaths() {
			return new String[]{CANCELLATION_IN_PROGRESS_REST_PATH};
		}
	}
}
