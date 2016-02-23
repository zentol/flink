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

import com.thinkaurelius.titan.core.TitanEdge;
import org.apache.flink.graph.Edge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public abstract class TitanEdgeSource<K, V> extends TitanSource<TitanEdge, Edge<K, V>> {
	protected GraphTraversal<TitanEdge, TitanEdge> getTraversal(GraphTraversalSource graph) {
		return getVertices(graph);
	}

	protected Edge<K, V> mapTitanType(TitanEdge titanObject) {
		return mapToEdge(titanObject);
	}

	protected abstract GraphTraversal<TitanEdge, TitanEdge> getVertices(GraphTraversalSource graph);

	protected abstract Edge<K, V> mapToEdge(TitanEdge vertex);
}
