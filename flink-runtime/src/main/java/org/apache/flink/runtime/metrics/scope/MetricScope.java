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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * TODO: add javadoc.
 */
public class MetricScope {

	private final MetricScope parent;

	public String[] getScopeComponents() {
		return scopeComponents;
	}

	/**
	 * The metrics scope represented by this group.
	 * For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ].
	 */
	private final String[] scopeComponents;

	/**
	 * Array containing the metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "host-7.taskmanager-2.window_word_count.my-mapper"
	 */
	private final String[] scopeStrings;

	/**
	 * The logical metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "taskmanager.job.task"
	 */
	private final String[] logicalScopeStrings;
	protected volatile Map<String, String> variables;

	private final Function<Integer, Character> delimiterProvider;
	private final boolean includeInLogicalScope;
	private final String groupName;
	private QueryScopeInfo queryServiceScopeInfo;

	public MetricScope(MetricScope parent, String[] scopeComponents, int numReporters, Function<Integer, Character> delimiterProvider, boolean includeInLogicalScope, String groupName) {
		this.parent = parent;
		this.scopeComponents = scopeComponents;
		this.scopeStrings = new String[numReporters];
		this.logicalScopeStrings = new String[numReporters];
		this.delimiterProvider = delimiterProvider;
		this.includeInLogicalScope = includeInLogicalScope;
		this.groupName = groupName;
	}

	public Map<String, String> getAllVariables() {
		if (variables == null) {
			Map<String, String> tmpVariables = new HashMap<>();
			putVariables(tmpVariables);
			if (parent != null) { // not true for Job-/TaskManagerMetricGroup
				tmpVariables.putAll(parent.getAllVariables());
			}
			variables = tmpVariables;
		}
		return variables;
	}

	protected void putVariables(Map<String, String> variables) {
	}

	public String getLogicalScope(CharacterFilter filter, int reporterIndex) {
		final char delimiter = delimiterProvider.apply(reporterIndex);

		return createAndCacheLogicalScope(filter, delimiter, reporterIndex);
	}

	public String getLogicalMetricIdentifier(final String metricName, final CharacterFilter filter, final int reporterIndex) {
		final char delimiter = delimiterProvider.apply(reporterIndex);

		return createAndCacheLogicalScope(filter, delimiter, reporterIndex) + delimiter + metricName;
	}
	
	private String createAndCacheLogicalScope(final CharacterFilter filter, final char delimiter, final int reporterIndex) {
		final String logicalScope = createLogicalScope(filter, delimiter, reporterIndex);
		if (reporterIndex < 0 || reporterIndex >= logicalScopeStrings.length) {
			if (logicalScopeStrings[reporterIndex] == null) {
				logicalScopeStrings[reporterIndex] = logicalScope;
			}
		}
		return logicalScope;
	}

	private String createLogicalScope(final CharacterFilter filter, final char delimiter, final int reportedIndex) {
		if (!includeInLogicalScope) {
			return parent.createAndCacheLogicalScope(filter, delimiter, reportedIndex);
		}

		final String groupName = filter.filterCharacters(this.groupName);
		return parent == null
			? groupName
			: parent.createAndCacheLogicalScope(filter, delimiter, reportedIndex) + delimiter + groupName;
	}

	public String getMetricIdentifier(String metricName, CharacterFilter filter, int reporterIndex) {
		final char delimiter = delimiterProvider.apply(reporterIndex);

		if (scopeStrings.length == 0 || (reporterIndex < 0 || reporterIndex >= scopeStrings.length)) {
			String newScopeString;
			if (filter != null) {
				newScopeString = ScopeFormat.concat(filter, delimiter, scopeComponents);
				metricName = filter.filterCharacters(metricName);
			} else {
				newScopeString = ScopeFormat.concat(delimiter, scopeComponents);
			}
			return newScopeString + delimiter + metricName;
		} else {
			if (scopeStrings[reporterIndex] == null) {
				if (filter != null) {
					scopeStrings[reporterIndex] = ScopeFormat.concat(filter, delimiter, scopeComponents);
				} else {
					scopeStrings[reporterIndex] = ScopeFormat.concat(delimiter, scopeComponents);
				}
			}
			if (filter != null) {
				metricName = filter.filterCharacters(metricName);
			}
			return scopeStrings[reporterIndex] + delimiter + metricName;
		}
	}

	public QueryScopeInfo getQueryServiceMetricInfo(CharacterFilter filter) {
		if (queryServiceScopeInfo == null) {
			queryServiceScopeInfo = createQueryServiceMetricInfo(filter);
		}
		return queryServiceScopeInfo;
	}

	/**
	 * Creates the metric query service scope for this group.
	 *
	 * @param filter character filter
	 * @return query service scope
	 */
	protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		return parent.getQueryServiceMetricInfo(filter).copy(filter.filterCharacters(this.groupName));
	}
}
