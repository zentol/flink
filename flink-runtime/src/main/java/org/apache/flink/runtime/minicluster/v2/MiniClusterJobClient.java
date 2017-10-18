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

package org.apache.flink.runtime.minicluster.v2;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * {@link JobClientV2} implementation for the {@link org.apache.flink.runtime.minicluster.MiniCluster}.
 */
public class MiniClusterJobClient implements JobClientV2 {
	private final JobID jobID;
	private final DispatcherGateway dispatcher;

	public MiniClusterJobClient(JobID jobID, DispatcherGateway dispatcher) {
		this.jobID = jobID;
		this.dispatcher = dispatcher;
	}

	@Override
	public CompletableFuture<Void> cancelJob() {
		return dispatcher.cancelJob(jobID, Time.seconds(5))
			.thenApply(ack -> null);
	}

	@Override
	public CompletableFuture<Void> stopJob() {
		return dispatcher.stopJob(jobID, Time.seconds(5))
			.thenApply(ack -> null);
	}

	@Override
	public void waitForJobTermination() {
		while (true) {
			try {
				dispatcher.getJobTerminationFuture(jobID, Time.seconds(5)).get();
				break;
			} catch (InterruptedException e) {
			} catch (ExecutionException e) {
			}
		}
	}
}
