/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util;

import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility class to terminate a given {@link Process} when exiting a try-with-resources statement.
 */
public class AutoClosableProcess implements AutoCloseable {

	private final Process process;

	public AutoClosableProcess(final Process process) {
		Preconditions.checkNotNull(process);
		this.process = process;
	}

	public Process getProcess() {
		return process;
	}

	public static AutoClosableProcess runNonBlocking(String... commands) throws IOException {
		return runNonBlocking(true, commands);
	}

	public static AutoClosableProcess runNonBlocking(boolean inheritIO, String... commands) throws IOException {
		return new AutoClosableProcess(createProcess(inheritIO, commands));
	}

	public static Process runBlocking(String... commands) throws IOException {
		return runBlocking(Duration.ofSeconds(30), commands);
	}

	public static Process runBlocking(boolean inheritIO, String... commands) throws IOException {
		return runBlocking(inheritIO, Duration.ofSeconds(30), commands);
	}

	public static Process runBlocking(Duration timeout, String... commands) throws IOException {
		return runBlocking(true, timeout, commands);
	}

	public static Process runBlocking(boolean inheritIO, Duration timeout, String... commands) throws IOException {
		final Process process = createProcess(inheritIO, commands);

		try (AutoClosableProcess autoProcess = new AutoClosableProcess(process)) {
			final boolean success = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
			if (!success) {
				throw new TimeoutException("Process exceeded timeout of " + timeout.getSeconds() + "seconds.");
			}
			if (process.exitValue() != 0) {
				throw new RuntimeException("Process execution failed due error.");
			}
		} catch (TimeoutException | InterruptedException e) {
			throw new RuntimeException("Process failed due to timeout.");
		}
		return process;
	}

	private static Process createProcess(boolean inheritIO, String... commands) throws IOException {
		final ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command(commands);
		if (inheritIO) {
			processBuilder.inheritIO();
		}
		return processBuilder.start();
	}

	@Override
	public void close() throws IOException {
		if (process.isAlive()) {
			process.destroy();
			try {
				process.waitFor(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
