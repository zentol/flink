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

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Restart strategy which tries to restart the given {@link ExecutionGraph} a fixed number of times
 * with a fixed time delay in between.
 */
public class FixedDelayRestartStrategy implements RestartStrategy {

	private final int maxNumberRestartAttempts;
	private final long delayBetweenRestartAttempts;
	private int currentRestartAttempt;

	public FixedDelayRestartStrategy(
		int maxNumberRestartAttempts,
		long delayBetweenRestartAttempts) {

		Preconditions.checkArgument(maxNumberRestartAttempts >= 0, "Maximum number of restart attempts must be positive.");
		Preconditions.checkArgument(delayBetweenRestartAttempts >= 0, "Delay between restart attempts must be positive");

		this.maxNumberRestartAttempts = maxNumberRestartAttempts;
		this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
		currentRestartAttempt = 0;
	}

	public int getCurrentRestartAttempt() {
		return currentRestartAttempt;
	}

	@Override
	public boolean canRestart() {
		return currentRestartAttempt < maxNumberRestartAttempts;
	}

	@Override
	public CompletableFuture<Void> restart(final RestartCallback restarter, ScheduledExecutor executor) {
		currentRestartAttempt++;
		return FutureUtils.scheduleWithDelay(restarter::triggerFullRecovery, Duration.ofMillis(delayBetweenRestartAttempts), executor);
	}

	/**
	 * Creates a FixedDelayRestartStrategy from the given Configuration.
	 *
	 * @param configuration Configuration containing the parameter values for the restart strategy
	 * @return Initialized instance of FixedDelayRestartStrategy
	 * @throws Exception
	 */
	public static FixedDelayRestartStrategyFactory createFactory(Configuration configuration) throws Exception {
		int maxAttempts = configuration.getInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);

		String delayString = configuration.getString(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);

		long delay;

		try {
			delay = TimeUtils.parseDuration(delayString).toMillis();
		} catch (IllegalArgumentException ex) {
			throw new Exception("Invalid config value for " +
					RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY.key() + ": " + delayString +
					". Value must be a valid duration (such as '100 milli' or '10 s')", ex);
		}

		return new FixedDelayRestartStrategyFactory(maxAttempts, delay);
	}

	@Override
	public String toString() {
		return "FixedDelayRestartStrategy(" +
				"maxNumberRestartAttempts=" + maxNumberRestartAttempts +
				", delayBetweenRestartAttempts=" + delayBetweenRestartAttempts +
				')';
	}

	public static class FixedDelayRestartStrategyFactory extends RestartStrategyFactory {

		private static final long serialVersionUID = 6642934067762271950L;

		private final int maxNumberRestartAttempts;
		private final long delayBetweenRestartAttempts;

		public FixedDelayRestartStrategyFactory(int maxNumberRestartAttempts, long delayBetweenRestartAttempts) {
			this.maxNumberRestartAttempts = maxNumberRestartAttempts;
			this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
		}

		@Override
		public RestartStrategy createRestartStrategy() {
			return new FixedDelayRestartStrategy(maxNumberRestartAttempts, delayBetweenRestartAttempts);
		}

		int getMaxNumberRestartAttempts() {
			return maxNumberRestartAttempts;
		}

		long getDelayBetweenRestartAttempts() {
			return delayBetweenRestartAttempts;
		}
	}
}
