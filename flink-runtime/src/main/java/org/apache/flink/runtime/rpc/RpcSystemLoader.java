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
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.core.plugin.PluginLoader;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/** Utils for loading the {@link RpcSystem}. */
public final class RpcSystemLoader {

    /**
     * Loads the RpcSystem.
     *
     * @return loaded RpcSystem
     */
    public static RpcSystem load() {
        return load(new Configuration());
    }

    /**
     * Loads the RpcSystem.
     *
     * @return loaded RpcSystem
     */
    public static RpcSystem load(Configuration config) {
        try {
            final ClassLoader classLoader = RpcSystem.class.getClassLoader();

            final String tmpDirectory = ConfigurationUtils.parseTempDirectories(config)[0];
            final Path tempFile =
                    Files.createFile(
                            Paths.get(tmpDirectory, UUID.randomUUID() + "_flink-rpc-akka.jar"));
            IOUtils.copyBytes(
                    classLoader.getResourceAsStream("flink-rpc-akka.jar"),
                    Files.newOutputStream(tempFile));

            final PluginLoader pluginLoader =
                    PluginLoader.create(
                            new PluginDescriptor(
                                    "flink-rpc-akka",
                                    new URL[] {tempFile.toUri().toURL()},
                                    new String[0]),
                            classLoader,
                            CoreOptions.getPluginParentFirstLoaderPatterns(config));
            return new PluginLoaderClosingRpcSystem(
                    pluginLoader.load(RpcSystem.class).next(), pluginLoader, tempFile);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize RPC system.", e);
        }
    }

    private RpcSystemLoader() {}
}
