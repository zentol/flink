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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction used by the {@link Dispatcher} to manage JobManagers, in
 * particular during initialization.
 * While a job is initializing, the JobMasterGateway is not available. A small subset
 * of the methods of the JobMasterGateway necessary during initialization are provided
 * by this class (job details, cancel).
 */
public final class DispatcherJob implements AutoCloseableAsync {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;
	private final CompletableFuture<ArchivedExecutionGraph> jobResultFuture;

	private final long initializationTimestamp;
	private final ComponentMainThreadExecutor mainThreadExecutor;
	private final JobID jobId;
	private final String jobName;

	// internal field to track job status during initialization. Is not updated anymore after
	// job is initialized, cancelled or failed.
	private JobStatus jobStatus = JobStatus.INITIALIZING;

	private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

	private enum SubmissionType {
		INITIAL, RECOVERY
	}

	static DispatcherJob createForSubmission(
		CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
		ComponentMainThreadExecutor mainThreadExecutor,
		JobID jobId,
		String jobName,
		long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, mainThreadExecutor, jobId, jobName, initializationTimestamp, SubmissionType.INITIAL);
	}

	static DispatcherJob createForRecovery(
		CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
		ComponentMainThreadExecutor mainThreadExecutor,
		JobID jobId,
		String jobName,
		long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, mainThreadExecutor, jobId, jobName, initializationTimestamp, SubmissionType.RECOVERY);
	}

	private DispatcherJob(
		CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
		ComponentMainThreadExecutor mainThreadExecutor,
		JobID jobId,
		String jobName,
		long initializationTimestamp,
		SubmissionType submissionType) {
		this.jobManagerRunnerFuture = jobManagerRunnerFuture;
		this.mainThreadExecutor = mainThreadExecutor;
		this.jobId = jobId;
		this.jobName = jobName;
		this.initializationTimestamp = initializationTimestamp;
		this.jobResultFuture = new CompletableFuture<>();

		FutureUtils.assertNoException(this.jobManagerRunnerFuture.handleAsync((jobManagerRunner, throwable) -> {
			// JM has been initialized, or the initialization failed
			if (throwable == null) {
				// Forward result future
				FutureUtils.forward(jobManagerRunner.getResultFuture(), jobResultFuture);

				if (jobStatus == JobStatus.CANCELLING) {
					log.warn(
						"JobManager initialization has been cancelled for job {}. Cancelling job.", jobId);

					CompletableFuture<Acknowledge> cancelJobFuture = jobManagerRunner.getJobMasterGateway().thenCompose(
						gw -> gw.cancel(RpcUtils.INF_TIMEOUT));
					// cancellation will eventually complete the jobResultFuture
					jobResultFuture.whenComplete((archivedExecutionGraph, resultThrowable) -> {
						if (resultThrowable == null) {
							jobStatus = archivedExecutionGraph.getState();
						} else {
							jobStatus = JobStatus.FAILED;
						}
					});
				} else {
					jobStatus = JobStatus.RUNNING; // this status should never be exposed from the DispatcherJob. Only used internally for tracking running state
				}
			} else { // failure during initialization
				jobStatus = JobStatus.FAILED;
				if (submissionType == SubmissionType.RECOVERY) {
					jobResultFuture.completeExceptionally(throwable);
				} else {
					jobResultFuture.complete(ArchivedExecutionGraph.createFromFailedInit(
						jobId,
						jobName,
						throwable,
						jobStatus,
						initializationTimestamp));
				}
			}
			return null;
		}, mainThreadExecutor));
	}

	public CompletableFuture<ArchivedExecutionGraph> getResultFuture() {
		return jobResultFuture;
	}

	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		mainThreadExecutor.assertRunningInMainThread();
		if (isRunning()) {
			return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.requestJobDetails(
				timeout));
		} else {
			int[] tasksPerState = new int[ExecutionState.values().length];
			return CompletableFuture.completedFuture(new JobDetails(
				jobId,
				jobName,
				initializationTimestamp,
				0,
				0,
				jobStatus,
				0,
				tasksPerState,
				0));
		}
	}

	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		mainThreadExecutor.assertRunningInMainThread();
		if (isRunning()) {
			return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.cancel(
				timeout));
		} else {
			jobStatus = JobStatus.CANCELLING;
			return jobResultFuture.thenApply(ignored -> Acknowledge.get());
		}
	}

	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		mainThreadExecutor.assertRunningInMainThread();
		if (isRunning()) {
			return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.requestJobStatus(timeout));
		} else {
			return CompletableFuture.completedFuture(jobStatus);
		}
	}

	public boolean isRunning() {
		return jobStatus == JobStatus.RUNNING;
	}

	public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
		mainThreadExecutor.assertRunningInMainThread();
		Preconditions.checkState(
			isRunning(),
			"JobMaster Gateway is not available during initialization");
		try {
			return jobManagerRunnerFuture.thenCompose(JobManagerRunner::getJobMasterGateway);
		} catch (Throwable e) {
			throw new IllegalStateException("JobMaster gateway is not available", e);
		}
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		mainThreadExecutor.assertRunningInMainThread();
		jobManagerRunnerFuture.handle((runner, throwable) -> {
			if (throwable == null) {
				// init was successful: close jobManager runner.
				CompletableFuture<Void> jobManagerRunnerClose = jobManagerRunnerFuture.thenCompose(
					AutoCloseableAsync::closeAsync);
				FutureUtils.forward(jobManagerRunnerClose, terminationFuture);
			} else {
				// initialization has failed. Termination complete.
				terminationFuture.complete(null);
			}
			return null;
		});
		return terminationFuture;
	}

}
