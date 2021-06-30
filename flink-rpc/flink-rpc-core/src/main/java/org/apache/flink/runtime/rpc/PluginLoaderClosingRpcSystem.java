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
import org.apache.flink.core.plugin.PluginLoader;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * An {@link RpcSystem} wrapper that closes the used {@link PluginLoader} when the RPC system is
 * being closed.
 */
class PluginLoaderClosingRpcSystem implements RpcSystem {
    private final RpcSystem rpcSystem;
    private final PluginLoader pluginLoader;

    public PluginLoaderClosingRpcSystem(RpcSystem rpcSystem, PluginLoader pluginLoader) {
        this.rpcSystem = rpcSystem;
        this.pluginLoader = pluginLoader;
    }

    @Override
    public void cleanup() {
        rpcSystem.cleanup();
        pluginLoader.close();
    }

    @Override
    public RpcServiceBuilder localServiceBuilder(Configuration config) {
        return rpcSystem.localServiceBuilder(config);
    }

    @Override
    public RpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return rpcSystem.remoteServiceBuilder(configuration, externalAddress, externalPortRange);
    }

    @Override
    public String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException {
        return rpcSystem.getRpcUrl(hostname, port, endpointName, addressResolution, config);
    }

    @Override
    public InetSocketAddress getInetSocketAddressFromRpcUrl(String url) throws Exception {
        return rpcSystem.getInetSocketAddressFromRpcUrl(url);
    }

    @Override
    public long getMaximumMessageSizeInBytes(Configuration config) {
        return rpcSystem.getMaximumMessageSizeInBytes(config);
    }
}
