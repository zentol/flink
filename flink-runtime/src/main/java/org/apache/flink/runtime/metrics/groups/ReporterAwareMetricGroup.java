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
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Package-private extension of the {@link MetricGroup} interface that adds methods which support reporter-specific
 * logic, with the primary user being the {@link FrontMetricGroup}.
 */
@Internal
abstract class ReporterAwareMetricGroup implements MetricGroup {

	abstract String getLogicalScope(CharacterFilter filter, int reporterIndex);

	abstract String getLogicalMetricIdentifier(final String metricName, final CharacterFilter filter, final int reporterIndex);

	abstract String getMetricIdentifier(String metricName, CharacterFilter filter, int reporterIndex);
}
