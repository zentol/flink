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

import org.apache.flink.metrics.View;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.metrics.View.UPDATE_INTERVAL_SECONDS;

/**
 * The ViewUpdater is responsible for updating all metrics that implement the {@link View} interface.
 */
public final class ViewUpdater {
	private final Set<View> toAdd = new HashSet<>();
	private final Set<View> toRemove = new HashSet<>();

	private final Object lock = new Object();
	private final ViewUpdaterThread thread;

	public ViewUpdater() {
		thread = new ViewUpdaterThread(lock, toAdd, toRemove);
		thread.start();
	}

	/**
	 * Notifies this ViewUpdater of a new metric that should be regularly updated.
	 *
	 * @param view metric that should be regularly updated
	 */
	public void notifyOfAddedView(View view) {
		synchronized (lock) {
			toAdd.add(view);
		}
	}

	/**
	 * Notifies this ViewUpdater of a metric that should no longer be regularly updated.
	 *
	 * @param view metric that should no longer be regularly updated
	 */
	public void notifyOfRemovedView(View view) {
		synchronized (lock) {
			toRemove.add(view);
		}
	}

	private static class ViewUpdaterThread extends Thread {
		private static final AtomicInteger threadNumber = new AtomicInteger(0);
		private final Object lock;
		private final Set<View> views;
		private final Set<View> toAdd;
		private final Set<View> toRemove;
		
		private volatile boolean running;

		private ViewUpdaterThread(Object lock, Set<View> toAdd, Set<View> toRemove) {
			super("Flink-MetricViewUpdaterThread-" + threadNumber.incrementAndGet());
			this.lock = lock;
			this.views = new HashSet<>();
			this.toAdd = toAdd;
			this.toRemove = toRemove;
		}

		@Override
		public void run() {
			running = true;
			long targetTime = System.currentTimeMillis() + UPDATE_INTERVAL_SECONDS * 1000;
			long currentTime;
			long timeDiff;
			while (running) {
				currentTime = System.currentTimeMillis();
				timeDiff = targetTime - currentTime;
				while (timeDiff < 0) {
					try {
						Thread.sleep(timeDiff);
					} catch (InterruptedException ignored) {
					}
					currentTime = System.currentTimeMillis();
					timeDiff = targetTime - currentTime;
				}
				targetTime = targetTime + UPDATE_INTERVAL_SECONDS * 1000;
				
				for (View toUpdate : this.views) {
					toUpdate.update();
				}

				synchronized (lock) {
					views.addAll(toAdd);
					toAdd.clear();
					views.removeAll(toRemove);
					toRemove.clear();
				}
			}
		}

		public void shutdown() {
			running = false;
		}
	}
}
