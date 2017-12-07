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
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler for {@link JobManagerMessages.TriggerSavepoint} messages.
 */
public class TriggerSavepointHandlers {

	private static final String TRIGGER_SAVEPOINT_REST_PATH = "/jobs/:jobid/trigger-savepoint";
	private static final String TRIGGER_SAVEPOINT_DIRECTORY_REST_PATH = "/jobs/:jobid/trigger-savepoint/target-directory/:targetDirectory";

	/** URL for in-progress cancellations. */
	private static final String SAVEPOINTS_IN_PROGRESS_REST_PATH = "/jobs/:jobid/trigger-savepoint/in-progress/:requestId";

	/** Handler for trigger requests. */
	private final TriggerHandler triggerHandler;

	/** Handler for in-progress requests. */
	private final InProgressHandler inProgressHandler;

	public TriggerSavepointHandlers(
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
			TRIGGER_SAVEPOINT_REST_PATH,
			TRIGGER_SAVEPOINT_DIRECTORY_REST_PATH,
			false,
			defaultSavepointDirectory,
			SAVEPOINTS_IN_PROGRESS_REST_PATH);
		this.inProgressHandler = new InProgressHandler(lock, completed, inProgress, SAVEPOINTS_IN_PROGRESS_REST_PATH);
	}

	public TriggerHandler getTriggerHandler() {
		return triggerHandler;
	}

	public InProgressHandler getInProgressHandler() {
		return inProgressHandler;
	}
}
