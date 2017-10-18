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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;

/**
 * {@link ClusterClientV2} implementation for the {@link MiniCluster}.
 */
public class MiniClusterClusterClient implements ClusterClientV2 {

	private final MiniCluster cluster;

	public MiniClusterClusterClient(MiniCluster cluster) {
		this.cluster = cluster;
	}

	@Override
	public JobClientV2 submitJob(JobGraph job) {
		// temporary hack for FLIP-6 since slot-sharing isn't implemented yet
		job.setAllowQueuedScheduling(true);
		DispatcherGateway dispatcher = cluster.getDispatcher();
		dispatcher.submitJob(job, Time.seconds(5));
		return new MiniClusterJobClient(job.getJobID(), dispatcher);
	}
}
