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

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: add javadoc.
 */
public abstract class Scope {

	/** The metrics scope represented by this group.
	 *  For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ]. */
	private final String[] scopeComponents;

	/** Array containing the metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "host-7.taskmanager-2.window_word_count.my-mapper" */
	private final String[] scopeStrings;

	/** The map containing all variables and their associated values, lazily computed. */
	protected volatile Map<String, String> variables;

	private final Scope parentScope;

	public Scope(String[] scopeComponents, String[] scopeStrings) {
		this.parentScope = null;
		this.scopeComponents = scopeComponents;
		this.scopeStrings = scopeStrings;
	}

	public Map<String, String> getAllVariables() {
		if (variables == null) { // avoid synchronization for common case
			synchronized (this) {
				if (variables == null) {
					variables = new HashMap<>();
					putVariables(variables);
					if (parentScope != null) { // not true for Job-/TaskManagerMetricGroup
						variables.putAll(parentScope.getAllVariables());
					}
				}
			}
		}
		return variables;
	}

	/**
	 * Enters all variables specific to this ComponentMetricGroup and their associated values into the map.
	 *
	 * @param variables map to enter variables and their values into
	 */
	protected abstract void putVariables(Map<String, String> variables);
}
