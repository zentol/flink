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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.runtime.rest.handler.legacy.DashboardConfigHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;

/**
 * Response of the {@link DashboardConfigHandler} containing general configuration
 * values such as the time zone and the refresh interval.
 */
public class DashboardConfiguration implements ResponseBody {

	public static final String FIELD_NAME_REFRESH_INTERVAL = "refresh-interval";
	public static final String FIELD_NAME_TIMEZONE_OFFSET = "timezone-offset";
	public static final String FIELD_NAME_TIMEZONE_NAME = "timezone-name";
	public static final String FIELD_NAME_FLINK_VERSION = "flink-version";
	public static final String FIELD_NAME_FLINK_REVISION = "flink-revision";

	@JsonProperty(FIELD_NAME_REFRESH_INTERVAL)
	private final long refreshInterval;

	@JsonProperty(FIELD_NAME_TIMEZONE_NAME)
	private final String timeZoneName;

	@JsonProperty(FIELD_NAME_TIMEZONE_OFFSET)
	private final int timeZoneOffset;

	@JsonProperty(FIELD_NAME_FLINK_VERSION)
	private final String flinkVersion;

	private final String flinkRevision;

	public DashboardConfiguration(
			long refreshInterval,
			String timeZoneName,
			int timeZoneOffset,
			String flinkVersion,
			String flinkRevision) {
		this.refreshInterval = refreshInterval;
		this.timeZoneName = Preconditions.checkNotNull(timeZoneName);
		this.timeZoneOffset = timeZoneOffset;
		this.flinkVersion = flinkVersion;
		this.flinkRevision = flinkRevision;
	}

	public long getRefreshInterval() {
		return refreshInterval;
	}

	public int getTimeZoneOffset() {
		return timeZoneOffset;
	}

	public String getTimeZoneName() {
		return timeZoneName;
	}

	public String getFlinkVersion() {
		return flinkVersion;
	}

	public String getFlinkRevision() {
		return flinkRevision;
	}

	public static DashboardConfiguration from(long refreshInterval, ZonedDateTime zonedDateTime) {

		final String flinkVersion = EnvironmentInformation.getVersion();

		final EnvironmentInformation.RevisionInformation revision = EnvironmentInformation.getRevisionInformation();
		final String flinkRevision;

		if (revision != null) {
			flinkRevision = revision.commitId + " @ " + revision.commitDate;
		} else {
			flinkRevision = "unknown revision";
		}

		return new DashboardConfiguration(
			refreshInterval,
			zonedDateTime.getZone().getDisplayName(TextStyle.FULL, Locale.getDefault()),
			// convert zone date time into offset in order to not do the day light saving adaptions wrt the offset
			zonedDateTime.toOffsetDateTime().getOffset().getTotalSeconds() * 1000,
			flinkVersion,
			flinkRevision);
	}
}
