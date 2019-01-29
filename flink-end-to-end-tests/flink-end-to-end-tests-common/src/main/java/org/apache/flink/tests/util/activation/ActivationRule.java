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

package org.apache.flink.tests.util.activation;

import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * TODO: add javadoc.
 */
public final class ActivationRule implements TestRule {

	private static final Logger LOG = LoggerFactory.getLogger(ActivationRule.class);

	private static final String GLOBAL_ACTIVATION_SWITCH = "e2e-all";
	private static final Character ACTIVATION_NEGATION_PREFIX = '!';

	final List<String> properties;

	public ActivationRule(final String ... properties) {
		this.properties = Arrays.asList(properties);
	}

	boolean areActivationPropertiesSet() {
		if (System.getProperty(GLOBAL_ACTIVATION_SWITCH) != null) {
			LOG.debug("Global activation property \"{}\" is set.", GLOBAL_ACTIVATION_SWITCH);
			return true;
		}

		final boolean anyActivationPropertySet = properties.stream()
			.filter(property -> System.getProperty(property) != null)
			.peek(property -> LOG.debug("Activation property \"{}\" is set.", property))
			.findAny()
			.isPresent();

		final boolean anyActivationPropertyNegated = properties.stream()
			.filter(property -> System.getProperty(ACTIVATION_NEGATION_PREFIX + property) != null)
			.peek(property -> LOG.debug("Activation property \"{}\" is negated.", property))
			.findAny()
			.isPresent();

		return anyActivationPropertySet && !anyActivationPropertyNegated;
	}

	@Override
	public Statement apply(final Statement base, final Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				Assume.assumeTrue(
					String.format("None of the activation properties (%s) were set.", properties),
					areActivationPropertiesSet());
				base.evaluate();
			}
		};
	}
}
