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

package org.apache.flink.runtime.dispatcher.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyParameters;
import org.apache.flink.runtime.rest.messages.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.JobSubmitResponseBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link JobSubmitHandler}.
 */
public class JobSubmitHandlerTest {

	@Test
	public void testSerializationFailureHandling() throws Exception {
		DispatcherGateway mockGateway = mock(DispatcherGateway.class);
		when(mockGateway.submitJob(any(JobGraph.class), any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT);

		JobSubmitRequestBody request = new JobSubmitRequestBody(new byte[0]);

		handler.handleRequest(new HandlerRequest<>(request, new EmptyParameters()), mockGateway);

		try {
			handler.handleRequest(new HandlerRequest<>(request, new EmptyParameters()), mockGateway).get();
			Assert.fail();
		} catch (ExecutionException ee) {
			RestHandlerException rhe = (RestHandlerException) ee.getCause();

			Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, rhe.getHttpResponseStatus());
		}
	}

	@Test
	public void testSuccessfulJobSubmission() throws Exception {
		DispatcherGateway mockGateway = mock(DispatcherGateway.class);
		when(mockGateway.submitJob(any(JobGraph.class), any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT);

		JobGraph job = new JobGraph("testjob");
		JobSubmitRequestBody request = new JobSubmitRequestBody(job);

		handler.handleRequest(new HandlerRequest<>(request, new EmptyParameters()), mockGateway).get();
	}
}
