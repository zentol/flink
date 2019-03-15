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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ScopeFormats}.
 */
public class ScopeFormatsTest {

	/**
	 * Verifies that the scope configuration is properly extracted.
	 */
	@Test
	public void testScopeConfig() {
		final Configuration config = new Configuration();

		config.setString(MetricOptions.SCOPE_NAMING_TM, "A");
		config.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
		config.setString(MetricOptions.SCOPE_NAMING_TASK, "C");
		config.setString(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

		final ScopeFormats scopeConfig = ScopeFormats.fromConfig(config);

		assertEquals("A", scopeConfig.getTaskManagerFormat().format());
		assertEquals("B", scopeConfig.getTaskManagerJobFormat().format());
		assertEquals("C", scopeConfig.getTaskFormat().format());
		assertEquals("D", scopeConfig.getOperatorFormat().format());
	}
}
