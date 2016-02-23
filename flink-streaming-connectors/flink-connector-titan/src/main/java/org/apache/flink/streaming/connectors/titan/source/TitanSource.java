/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.titan.source;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.IOException;

public abstract class TitanSource<BT, T> extends RichInputFormat<T, InputSplit> implements NonParallelInput {
	protected transient TitanGraph graph;
	protected transient GraphTraversal<BT, BT> traversal;

	protected abstract GraphTraversal<BT, BT> getTraversal(GraphTraversalSource graph);

	@Override
	public void open(InputSplit split) throws IOException {
		BaseConfiguration baseConfiguration = new BaseConfiguration();
		graph = TitanFactory.open(baseConfiguration);
		traversal = getTraversal(graph.traversal());
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !traversal.hasNext();
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		BT titanObject = traversal.next();
		return mapTitanType(titanObject);
	}

	protected abstract T mapTitanType(BT titanObject);

	@Override
	public void close() throws IOException {
		graph.close();
	}

	//trash
	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return new InputSplit[0];
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return null;
	}
}
