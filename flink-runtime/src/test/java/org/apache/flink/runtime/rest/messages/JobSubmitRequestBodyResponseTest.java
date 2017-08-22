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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link JobSubmitRequestBody} and {@link JobSubmitResponseBody}.
 */
public class JobSubmitRequestBodyResponseTest {

	private static final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

	@Test
	public void testJsonCompatibility() throws IOException {
		JobSubmitRequestBody request = new JobSubmitRequestBody(new JobGraph("job"));

		JsonNode requestJson = objectMapper.valueToTree(request);

		JobSubmitRequestBody readRequest = objectMapper.treeToValue(requestJson, JobSubmitRequestBody.class);
		Assert.assertEquals(request, readRequest);

		final String url = "/url";
		JobSubmitResponseBody response = new JobSubmitResponseBody(url);

		JsonNode responseJson = objectMapper.valueToTree(response);
		Assert.assertEquals(url, responseJson.get(JobSubmitResponseBody.FIELD_NAME_JOB_URL).asText());

		JobSubmitResponseBody readResponse = objectMapper.treeToValue(responseJson, JobSubmitResponseBody.class);
		Assert.assertEquals(response, readResponse);
	}
}
