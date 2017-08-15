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

package org.apache.flink.runtime.webmonitor.retriever;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.RpcGateway;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Retrieves and stores the leading {@link RpcGateway}.
 *
 * @param <T> type of the gateway to retrieve
 */
public abstract class LeaderGatewayRetriever<T extends RpcGateway> extends LeaderRetriever implements GatewayRetriever<T> {

	private volatile CompletableFuture<T> gatewayFuture;

	public LeaderGatewayRetriever() {
		gatewayFuture = createGateway(getLeaderFuture());
	}

	@Override
	public CompletableFuture<T> getFuture() {
		return gatewayFuture;
	}

	@Override
	public CompletableFuture<Tuple2<String, UUID>> createNewFuture() {
		CompletableFuture<Tuple2<String, UUID>> newFuture = super.createNewFuture();

		gatewayFuture = createGateway(newFuture);

		return newFuture;
	}

	protected abstract CompletableFuture<T> createGateway(CompletableFuture<Tuple2<String, UUID>> leaderFuture);
}
