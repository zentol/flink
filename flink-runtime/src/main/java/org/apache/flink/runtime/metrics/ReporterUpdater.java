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
package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.reporter.Scheduled;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ReporterUpdater {
	private final ReporterReportThread thread;

	public ReporterUpdater(List<Scheduled> reporters) {
		thread = new ReporterReportThread(reporters);
		thread.start();
	}

	private static class ReporterReportThread extends Thread {
		private static final AtomicInteger threadNumber = new AtomicInteger(0);
		private final List<Scheduled> reporters;

		private volatile boolean running;

		public ReporterReportThread(List<Scheduled> reporters) {
			super("Flink-MetricReporterReportThread-" + threadNumber.incrementAndGet());
			this.reporters = reporters;
		}

		@Override
		public void run() {
			
		}

		public void shutdown() {
			running = false;
		}
	}
}
