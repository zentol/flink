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
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJobWithSavepoint;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

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
			ExecutionGraphCache currentGraphs,
			Executor executor,
			@Nullable String defaultSavepointDirectory) {

		/* In-Progress requests. */
		Map<JobID, Long> inProgress = new HashMap<>();
		/* Succeeded/failed request. Either String or Throwable. */
		Map<Long, Object> completed = new HashMap<>();
		/* Shared lock between Trigger and In-Progress handlers. */
		Object lock = new Object();

		this.triggerHandler = new TriggerHandler(
			currentGraphs,
			executor,
			lock,
			completed,
			inProgress,
			CANCEL_WITH_SAVEPOINT_REST_PATH,
			CANCEL_WITH_SAVEPOINT_DIRECTORY_REST_PATH,
			true, defaultSavepointDirectory,
			CANCELLATION_IN_PROGRESS_REST_PATH);
		this.inProgressHandler = new InProgressHandler(lock, completed, inProgress, CANCELLATION_IN_PROGRESS_REST_PATH);
	}

	public TriggerHandler getTriggerHandler() {
		return triggerHandler;
	}

	public InProgressHandler getInProgressHandler() {
		return inProgressHandler;
	}
}
