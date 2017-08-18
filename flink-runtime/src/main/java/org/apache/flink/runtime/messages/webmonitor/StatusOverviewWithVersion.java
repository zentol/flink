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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Status overview message including the current flink version and commit id.
 */
public class StatusOverviewWithVersion extends StatusOverview implements ResponseBody {

	private static final long serialVersionUID = 5000058311783413216L;

	public static final String FIELD_NAME_VERSION = "flink-version";
	public static final String FIELD_NAME_COMMIT = "flink-commit";

	@JsonProperty(FIELD_NAME_VERSION)
	private final String version;

	@JsonProperty(FIELD_NAME_COMMIT)
	private final String commitId;

	public StatusOverviewWithVersion(
			int numTaskManagersConnected,
			int numSlotsTotal,
			int numSlotsAvailable,
			int numJobsRunningOrPending,
			int numJobsFinished,
			int numJobsCancelled,
			int numJobsFailed,
			String version,
			String commitId) {
		super(numTaskManagersConnected, numSlotsTotal, numSlotsAvailable, numJobsRunningOrPending, numJobsFinished, numJobsCancelled, numJobsFailed);

		this.version = Preconditions.checkNotNull(version);
		this.commitId = Preconditions.checkNotNull(commitId);
	}

	public StatusOverviewWithVersion(
			int numTaskManagersConnected,
			int numSlotsTotal,
			int numSlotsAvailable,
			JobsOverview jobs1,
			JobsOverview jobs2,
			String version,
			String commitId) {
		super(numTaskManagersConnected, numSlotsTotal, numSlotsAvailable, jobs1, jobs2);

		this.version = Preconditions.checkNotNull(version);
		this.commitId = Preconditions.checkNotNull(commitId);
	}

	public static StatusOverviewWithVersion fromStatusOverview(StatusOverview statusOverview, String version, String commitId) {
		return new StatusOverviewWithVersion(
			statusOverview.getNumTaskManagersConnected(),
			statusOverview.getNumSlotsTotal(),
			statusOverview.getNumSlotsAvailable(),
			statusOverview.getNumJobsRunningOrPending(),
			statusOverview.getNumJobsFinished(),
			statusOverview.getNumJobsCancelled(),
			statusOverview.getNumJobsFailed(),
			version,
			commitId);
	}

	public String getVersion() {
		return version;
	}

	public String getCommitId() {
		return commitId;
	}
}
