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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing everything belonging to
 * a specific job, running on the JobManager.
 */
@Internal
public class JobManagerJobMetricGroup extends JobMetricGroup<JobManagerMetricGroup> {
	@Nullable
	private final ResourceID jobManagerId;

	public JobManagerJobMetricGroup(
			MetricRegistry registry,
			JobManagerMetricGroup parent,
			JobID jobId,
			@Nullable String jobName,
			@Nullable ResourceID jobManagerId) {
		super(registry, checkNotNull(parent), jobId, jobName, registry.getScopeFormats().getJobManagerJobFormat().formatScope(checkNotNull(parent), jobId, jobName, jobManagerId));
		this.jobManagerId = jobManagerId;
	}

	public final JobManagerMetricGroup parent() {
		return parent;
	}

	// ------------------------------------------------------------------------
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	@Override
	protected void putVariables(Map<String, String> variables) {
		super.putVariables(variables);
		if (jobManagerId != null) {
			variables.put(ScopeFormat.SCOPE_JOBMANAGER_ID, jobManagerId.getResourceIdString());
		}
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return Collections.emptyList();
	}
}
