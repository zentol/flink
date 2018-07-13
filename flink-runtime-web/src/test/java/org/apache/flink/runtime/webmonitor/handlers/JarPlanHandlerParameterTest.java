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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Tests for the parameter handling of the {@link JarPlanHandler}.
 */
public class JarPlanHandlerParameterTest {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	private static JarPlanHandler handler;
	private static Path jarWithManifest;
	private static Path jarWithoutManifest;
	private static TestingDispatcherGateway restfulGateway;

	@BeforeClass
	public static void setup() throws Exception {
		Path jarDir = TMP.newFolder().toPath();

		// properties are set property by surefire plugin
		final String parameterProgramJarName = System.getProperty("parameterJarName") + ".jar";
		final String parameterProgramWithoutManifestJarName = System.getProperty("parameterJarWithoutManifestName") + ".jar";
		final Path jarLocation = Paths.get(System.getProperty("targetDir"));

		jarWithManifest = Files.copy(
			jarLocation.resolve(parameterProgramJarName),
			jarDir.resolve("program-with-manifest.jar"));
		jarWithoutManifest = Files.copy(
			jarLocation.resolve(parameterProgramWithoutManifestJarName),
			jarDir.resolve("program-without-manifest.jar"));

		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			TMP.newFolder().getAbsolutePath());

		restfulGateway = new TestingDispatcherGateway.Builder().build();
		final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
		final CompletableFuture<String> localAddressFuture = CompletableFuture.completedFuture("shazam://localhost:12345");
		final Time timeout = Time.seconds(10);
		final Map<String, String> responseHeaders = Collections.emptyMap();
		final Executor executor = TestingUtils.defaultExecutor();

		handler = new JarPlanHandler(
			localAddressFuture,
			gatewayRetriever,
			timeout,
			responseHeaders,
			JarPlanHeaders.getInstance(),
			jarDir,
			new Configuration(),
			executor);
	}

	@Before
	public void reset() {
		ParameterProgram.actualArguments = null;
	}

	@Test
	public void testDefaultParameters() throws Exception {
		// baseline, ensure that reasonable defaults are chosen
		sendRequestAndValidatePlan(
			handler,
			restfulGateway,
			() -> createRequest(
				new JarPlanRequestBody(),
				JarPlanHeaders.getInstance().getUnresolvedMessageParameters(),
				jarWithManifest
			),
			() -> {
				Assert.assertEquals(0, ParameterProgram.actualArguments.length);
			}
		);
	}

	@Test
	public void testConfigurationViaQueryParameters() throws Exception {
		// configure submission via query parameters
		sendRequestAndValidatePlan(
			handler,
			restfulGateway,
			() -> {
				final JarPlanMessageParameters parameters = JarPlanHeaders.getInstance().getUnresolvedMessageParameters();
				parameters.entryClassQueryParameter.resolve(Collections.singletonList(ParameterProgram.class.getCanonicalName()));
				parameters.parallelismQueryParameter.resolve(Collections.singletonList(4));
				parameters.programArgsQueryParameter.resolve(Collections.singletonList("--host localhost --port 1234"));

				return createRequest(
					new JarPlanRequestBody(),
					parameters,
					jarWithoutManifest
				);
			},
			() -> {
				Assert.assertEquals(4, ParameterProgram.actualArguments.length);
				Assert.assertEquals("--host", ParameterProgram.actualArguments[0]);
				Assert.assertEquals("localhost", ParameterProgram.actualArguments[1]);
				Assert.assertEquals("--port", ParameterProgram.actualArguments[2]);
				Assert.assertEquals("1234", ParameterProgram.actualArguments[3]);
			}
		);
	}

	@Test
	public void testConfigurationViaJsonRequest() throws Exception {
		sendRequestAndValidatePlan(
			handler,
			restfulGateway,
			() -> {
				final JarPlanRequestBody jsonRequest = new JarPlanRequestBody(
					ParameterProgram.class.getCanonicalName(),
					"--host localhost --port 1234",
					4
				);

				return createRequest(
					jsonRequest,
					JarPlanHeaders.getInstance().getUnresolvedMessageParameters(),
					jarWithoutManifest
				);
			},
			() -> {
				Assert.assertEquals(4, ParameterProgram.actualArguments.length);
				Assert.assertEquals("--host", ParameterProgram.actualArguments[0]);
				Assert.assertEquals("localhost", ParameterProgram.actualArguments[1]);
				Assert.assertEquals("--port", ParameterProgram.actualArguments[2]);
				Assert.assertEquals("1234", ParameterProgram.actualArguments[3]);
			}
		);
	}

	@Test
	public void testParameterPrioritization() throws Exception {
		// configure submission via query parameters and JSON request, JSON should be prioritized
		sendRequestAndValidatePlan(
			handler,
			restfulGateway,
			() -> {
				final JarPlanRequestBody jsonRequest = new JarPlanRequestBody(
					ParameterProgram.class.getCanonicalName(),
					"--host localhost --port 1234",
					4
				);

				final JarPlanMessageParameters parameters = JarPlanHeaders.getInstance().getUnresolvedMessageParameters();
				parameters.entryClassQueryParameter.resolve(Collections.singletonList("please.dont.run.me"));
				parameters.parallelismQueryParameter.resolve(Collections.singletonList(64));
				parameters.programArgsQueryParameter.resolve(Collections.singletonList("--host wrong --port wrong"));

				return createRequest(
					jsonRequest,
					parameters,
					jarWithoutManifest
				);
			},
			() -> {
				Assert.assertEquals(4, ParameterProgram.actualArguments.length);
				Assert.assertEquals("--host", ParameterProgram.actualArguments[0]);
				Assert.assertEquals("localhost", ParameterProgram.actualArguments[1]);
				Assert.assertEquals("--port", ParameterProgram.actualArguments[2]);
				Assert.assertEquals("1234", ParameterProgram.actualArguments[3]);
			}
		);
	}

	private static HandlerRequest<JarPlanRequestBody, JarPlanMessageParameters> createRequest(
			JarPlanRequestBody requestBody,
			JarPlanMessageParameters parameters,
			Path jar) throws HandlerRequestException {

		final Map<String, List<String>> queryParameterAsMap = parameters.getQueryParameters().stream()
			.filter(MessageParameter::isResolved)
			.collect(Collectors.toMap(
				MessageParameter::getKey,
				JarPlanHandlerParameterTest::getValuesAsString
			));

		return new HandlerRequest<>(
			requestBody,
			JarPlanHeaders.getInstance().getUnresolvedMessageParameters(),
			Collections.singletonMap(JarIdPathParameter.KEY, jar.getFileName().toString()),
			queryParameterAsMap,
			Collections.emptyList()
		);
	}

	private static void sendRequestAndValidatePlan(
			JarPlanHandler handler,
			DispatcherGateway dispatcherGateway,
			SupplierWithException<HandlerRequest<JarPlanRequestBody, JarPlanMessageParameters>, HandlerRequestException> requestSupplier,
			ThrowingRunnable<AssertionError> validator) throws Exception {

		handler.handleRequest(requestSupplier.get(), dispatcherGateway)
			.get();

		validator.run();
	}

	private static <X> List<String> getValuesAsString(MessageQueryParameter<X> parameter) {
		final List<X> values = parameter.getValue();
		return values.stream().map(parameter::convertValueToString).collect(Collectors.toList());
	}
}
