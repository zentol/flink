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
package org.apache.flink.streaming.connectors.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public abstract class test {
	public static void main(String args[]) {
		BaseConfiguration baseConfiguration = new BaseConfiguration();
		baseConfiguration.setProperty("storage.backend", "cassandra");
		baseConfiguration.setProperty("storage.hostname", "192.168.1.10");

		TitanGraph titanGraph = TitanFactory.open(baseConfiguration);
		TitanTransaction transaction = titanGraph.buildTransaction().start();

		TitanVertex rash = transaction.addVertex();
		rash.property("userId", 1);
		rash.property("username", "rash");
		rash.property("firstName", "Rahul");
		rash.property("lastName", "Chaudhary");
		rash.property("birthday", 101);

		TitanVertex honey = transaction.addVertex();
		honey.property("userId", 2);
		honey.property("username", "honey");
		honey.property("firstName", "Honey");
		honey.property("lastName", "Anant");
		honey.property("birthday", 201);

		TitanEdge frnd = rash.addEdge("FRIEND", honey);
		frnd.property("since", 2011);

		transaction.commit();

		GraphTraversal<Edge, Edge> edges = titanGraph.traversal().E();
		while (edges.hasNext()) {
			Edge edge = edges.next();
		}
		GraphTraversal<Vertex, Vertex> vertices = titanGraph.traversal().V();
		while (vertices.hasNext()) {
			Vertex vertex = vertices.next();
		}
	}
}
