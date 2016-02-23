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

import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.flink.graph.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public abstract class TitanVertexSource<K, V> extends TitanSource<TitanVertex, Vertex<K, V>> {
	protected GraphTraversal<TitanVertex, TitanVertex> getTraversal(GraphTraversalSource graph) {
		return getVertices(graph);
	}

	protected Vertex<K, V> mapTitanType(TitanVertex titanObject) {
		return mapToVertex(titanObject);
	}

	protected abstract GraphTraversal<TitanVertex, TitanVertex> getVertices(GraphTraversalSource graph);

	protected abstract Vertex<K, V> mapToVertex(TitanVertex vertex);
}
