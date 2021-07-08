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

package org.apache.flink.runtime.rpc;

import org.apache.flink.configuration.Configuration;

import java.util.ServiceLoader;

/** A loader for an {@link RpcSystem}. */
public interface RpcSystemLoader {
    RpcSystem loadRpcSystem(Configuration config);

    /**
     * Loads the RpcSystem.
     *
     * @return loaded RpcSystem
     */
    static RpcSystem load() {
        return load(new Configuration());
    }

    /**
     * Loads the RpcSystem.
     *
     * @param config Flink configuration
     * @return loaded RpcSystem
     */
    static RpcSystem load(Configuration config) {
        return ServiceLoader.load(RpcSystemLoader.class).iterator().next().loadRpcSystem(config);
    }
}
