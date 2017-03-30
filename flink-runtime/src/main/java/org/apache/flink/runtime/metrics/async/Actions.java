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
package org.apache.flink.runtime.metrics.async;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class Actions {

	public static final Registrar registrar = new Registrar();
	
	public static void main(String[] args) {
		new Thread(registrar).start();
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		TaskManagerMetricGroup tm = new TaskManagerMetricGroup(registry, "hello", "world");
		
		FutureProxyMetricGroup initialProxy = new FutureProxyMetricGroup();
		initialProxy.complete(tm);
		
		MetricGroup proxy = initialProxy;
		
		MetricGroup a = proxy.addGroup("g1");
		MetricGroup b = a.addGroup("g2");
		System.out.println(b.getMetricIdentifier("metric"));
		
		registrar.stop();
	}

	public enum X {
		ADD_GROUP,
		CLOSE_GROUP,
		ADD_METRIC,
		STOP
	}

	public abstract X getType();

	public static class AddGroup extends Actions {
		private final FutureProxyMetricGroup future;
		private final FutureProxyMetricGroup proxy;
		private final String name;

		public AddGroup(FutureProxyMetricGroup future, FutureProxyMetricGroup proxy, String name) {
			this.future = future;
			this.proxy = proxy;
			this.name = name;
		}

		@Override
		public X getType() {
			return X.ADD_GROUP;
		}
	}

	public static class CloseGroup extends Actions {
		private final FutureProxyMetricGroup proxy;

		public CloseGroup(FutureProxyMetricGroup proxy) {
			this.proxy = proxy;
		}

		@Override
		public X getType() {
			return X.CLOSE_GROUP;
		}
	}

	public static class AddMetric extends Actions {
		private final FutureProxyMetricGroup proxy;
		private final Metric metric;
		private final String name;

		public AddMetric(FutureProxyMetricGroup proxy, Metric metric, String name) {
			this.proxy = proxy;
			this.metric = metric;
			this.name = name;
		}

		@Override
		public X getType() {
			return X.ADD_METRIC;
		}
	}

	public static class Stop extends Actions {

		@Override
		public X getType() {
			return X.STOP;
		}
	}

	public static class Registrar implements Runnable {
		private final Queue<Actions> actionsQueue = new LinkedBlockingQueue<>();
		private volatile boolean running = true;

		public MetricGroup addGroup(FutureProxyMetricGroup parent, String name) {
			FutureProxyMetricGroup future = new FutureProxyMetricGroup();
			actionsQueue.add(new AddGroup(future, parent, name));
			return future;
		}

		public void closeGroup(FutureProxyMetricGroup proxy) {
			actionsQueue.add(new CloseGroup(proxy));
		}

		public <C extends Counter> C addCounter(FutureProxyMetricGroup proxy, C counter, String name) {
			actionsQueue.add(new AddMetric(proxy, counter, name));
			return counter;
		}

		public <G extends Gauge<?>> G addGauge(FutureProxyMetricGroup proxy, G gauge, String name) {
			actionsQueue.add(new AddMetric(proxy, gauge, name));
			return gauge;
		}

		public <H extends Histogram> H addHistogram(FutureProxyMetricGroup proxy, H histogram, String name) {
			actionsQueue.add(new AddMetric(proxy, histogram, name));
			return histogram;
		}

		public <M extends Meter> M addMeter(FutureProxyMetricGroup proxy, M meter, String name) {
			actionsQueue.add(new AddMetric(proxy, meter, name));
			return meter;
		}

		public void stop() {
			this.running = false;
		}

		@Override
		public void run() {
			while (running) {
				Actions action = actionsQueue.poll();
				if (action != null) {
					switch (action.getType()) {
						case ADD_GROUP:
							AddGroup addGroup = (AddGroup) action;
							AbstractMetricGroup<?> group = addGroup.proxy.parent.addGroup(addGroup.name);
							addGroup.future.complete(group);
							break;
						case CLOSE_GROUP:
							CloseGroup closeGroup = (CloseGroup) action;
							closeGroup.proxy.parent.close();
							break;
						case ADD_METRIC:
							AddMetric addMetric = (AddMetric) action;
							addMetric.proxy.parent.addMetric(addMetric.name, addMetric.metric);
							break;
						case STOP:
							return;
					}
				}
			}
		}
	}

	private static class FutureProxyMetricGroup implements MetricGroup {

		private final Object lock = new Object();

		private AbstractMetricGroup<?> parent;

		public void complete(AbstractMetricGroup<?> group) {
			synchronized (lock) {
				parent = group;
				lock.notifyAll();
			}
		}

		private void waitX() {
			synchronized (lock) {
				while (parent == null) {
					try {
						lock.wait();
					} catch (InterruptedException ignored) {
						Thread.interrupted();
					}
				}
			}
		}

		@Override
		public Counter counter(int name) {
			return counter(String.valueOf(name));
		}

		@Override
		public SimpleCounter counter(String name) {
			return counter(name, new SimpleCounter());
		}

		@Override
		public <C extends Counter> C counter(int name, C counter) {
			return counter(String.valueOf(name), counter);
		}

		@Override
		public <C extends Counter> C counter(String name, C counter) {
			return registrar.addCounter(this, counter, name);
		}

		@Override
		public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
			return gauge(String.valueOf(name), gauge);
		}

		@Override
		public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
			return registrar.addGauge(this, gauge, name);
		}

		@Override
		public <H extends Histogram> H histogram(int name, H histogram) {
			return histogram(String.valueOf(name), histogram);
		}

		@Override
		public <H extends Histogram> H histogram(String name, H histogram) {
			return registrar.addHistogram(this, histogram, name);
		}

		@Override
		public <M extends Meter> M meter(int name, M meter) {
			return meter(String.valueOf(name), meter);
		}

		@Override
		public <M extends Meter> M meter(String name, M meter) {
			return registrar.addMeter(this, meter, name);
		}

		@Override
		public MetricGroup addGroup(int name) {
			return registrar.addGroup(this, String.valueOf(name));
		}

		@Override
		public MetricGroup addGroup(String name) {
			return registrar.addGroup(this, name);
		}

		@Override
		public String[] getScopeComponents() {
			synchronized (lock) {
				waitX();
				return parent.getScopeComponents();
			}
		}

		@Override
		public Map<String, String> getAllVariables() {
			synchronized (lock) {
				waitX();
				return parent.getAllVariables();
			}
		}

		@Override
		public String getMetricIdentifier(String metricName) {
			synchronized (lock) {
				waitX();
				return parent.getMetricIdentifier(metricName);
			}
		}

		@Override
		public String getMetricIdentifier(String metricName, CharacterFilter filter) {
			synchronized (lock) {
				waitX();
				return parent.getMetricIdentifier(metricName, filter);
			}
		}
	}

}
