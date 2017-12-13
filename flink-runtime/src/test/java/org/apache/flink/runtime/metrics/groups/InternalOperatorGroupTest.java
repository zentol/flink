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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link InternalOperatorMetricGroup}.
 */
public class InternalOperatorGroupTest extends TestLogger {

	@Test
	public void testGenerateScopeDefault() {
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		MetricGroup opGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id")
			.addTaskForJob(new JobID(), "myJobName", new JobVertexID(), new ExecutionAttemptID(), "aTaskName", 11, 0)
			.addOperator(new OperatorID(), "myOpName");

		assertArrayEquals(
				new String[] { "theHostName", "taskmanager", "test-tm-id", "myJobName", "myOpName", "11" },
				opGroup.getScopeComponents());

		assertEquals(
				"theHostName.taskmanager.test-tm-id.myJobName.myOpName.11.name",
				opGroup.getMetricIdentifier("name"));
	}

	@Test
	public void testGenerateScopeCustom() {
		Configuration cfg = new Configuration();
		cfg.setString(MetricOptions.SCOPE_NAMING_OPERATOR, "<tm_id>.<job_id>.<task_id>.<operator_name>.<operator_id>");
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(cfg));
		try {
			String tmID = "test-tm-id";
			JobID jid = new JobID();
			JobVertexID vertexId = new JobVertexID();
			OperatorID operatorID = new OperatorID();
			String operatorName = "operatorName";

			MetricGroup operatorGroup = new TaskManagerMetricGroup(registry, "theHostName", tmID)
				.addTaskForJob(jid, "myJobName", vertexId, new ExecutionAttemptID(), "aTaskname", 13, 2)
				.addOperator(operatorID, operatorName);

			assertArrayEquals(
				new String[]{tmID, jid.toString(), vertexId.toString(), operatorName, operatorID.toString()},
				operatorGroup.getScopeComponents());

			assertEquals(
				String.format("%s.%s.%s.%s.%s.name", tmID, jid, vertexId, operatorName, operatorID),
				operatorGroup.getMetricIdentifier("name"));
		} finally {
			registry.shutdown();
		}
	}

	@Test
	public void testIOMetricGroupInstantiation() {
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		OperatorMetricGroup opGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id")
			.addTaskForJob(new JobID(), "myJobName", new JobVertexID(), new ExecutionAttemptID(), "aTaskName", 11, 0)
			.addOperator(new OperatorID(), "myOpName");

		assertNotNull(opGroup.getIOMetrics());
		assertNotNull(opGroup.getIOMetrics().getNumRecordsInCounter());
		assertNotNull(opGroup.getIOMetrics().getNumRecordsOutCounter());

		registry.shutdown();
	}

	@Test
	public void testVariables() {
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		JobID jid = new JobID();
		JobVertexID tid = new JobVertexID();
		AbstractID eid = new AbstractID();
		OperatorID oid = new OperatorID();

		MetricGroup opGroup = new TaskManagerMetricGroup(registry, "theHostName", "test-tm-id")
			.addTaskForJob(new JobID(), "myJobName", new JobVertexID(), new ExecutionAttemptID(), "aTaskName", 11, 0)
			.addOperator(new OperatorID(), "myOpName");

		Map<String, String> variables = opGroup.getAllVariables();

		testVariable(variables, ScopeFormat.SCOPE_HOST, "theHostName");
		testVariable(variables, ScopeFormat.SCOPE_TASKMANAGER_ID, "test-tm-id");
		testVariable(variables, ScopeFormat.SCOPE_JOB_ID, jid.toString());
		testVariable(variables, ScopeFormat.SCOPE_JOB_NAME, "myJobName");
		testVariable(variables, ScopeFormat.SCOPE_TASK_VERTEX_ID, tid.toString());
		testVariable(variables, ScopeFormat.SCOPE_TASK_NAME, "aTaskName");
		testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_ID, eid.toString());
		testVariable(variables, ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, "11");
		testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, "0");
		testVariable(variables, ScopeFormat.SCOPE_OPERATOR_ID, oid.toString());
		testVariable(variables, ScopeFormat.SCOPE_OPERATOR_NAME, "myOpName");

		registry.shutdown();
	}

	private static void testVariable(Map<String, String> variables, String key, String expectedValue) {
		String actualValue = variables.get(key);
		assertNotNull(actualValue);
		assertEquals(expectedValue, actualValue);
	}

	@Test
	public void testCreateQueryServiceMetricInfo() {
		JobID jid = new JobID();
		JobVertexID vid = new JobVertexID();
		ExecutionAttemptID eid = new ExecutionAttemptID();
		OperatorID oid = new OperatorID();
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		InternalOperatorMetricGroup operator = new TaskManagerMetricGroup(registry, "host", "id")
			.addTaskForJob(jid, "jobname", vid, eid, "aTaskName", 4, 5)
			.addOperator(oid, "operator");

		QueryScopeInfo.OperatorQueryScopeInfo info = operator.createQueryServiceMetricInfo(new DummyCharacterFilter());
		assertEquals("", info.scope);
		assertEquals(jid.toString(), info.jobID);
		assertEquals(vid.toString(), info.vertexID);
		assertEquals(4, info.subtaskIndex);
		assertEquals("operator", info.operatorName);
	}
}
