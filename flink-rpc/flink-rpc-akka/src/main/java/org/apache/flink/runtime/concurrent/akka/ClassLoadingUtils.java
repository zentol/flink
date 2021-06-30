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

package org.apache.flink.runtime.concurrent.akka;

import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.function.SupplierWithException;

/** Classloading utilities. */
public class ClassLoadingUtils {

    /**
     * Wraps the given runnable in a {@link TemporaryClassLoaderContext} to prevent the plugin class
     * loader from leaking into Flink.
     *
     * @param runnable runnable to wrap
     * @return wrapped runnable
     */
    public static Runnable withFlinkContextClassLoader(Runnable runnable) {
        return () -> runWithFlinkContextClassLoader(runnable);
    }

    /**
     * Runs the given runnable in a {@link TemporaryClassLoaderContext} to prevent the plugin class
     * loader from leaking into Flink.
     *
     * @param runnable runnable to run
     */
    public static void runWithFlinkContextClassLoader(Runnable runnable) {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(RpcService.class.getClassLoader())) {
            runnable.run();
        }
    }

    /**
     * Runs the given supplier in a {@link TemporaryClassLoaderContext} to prevent the plugin class
     * loader from leaking into Flink.
     *
     * @param supplier supplier to run
     */
    public static <T, E extends Throwable> T runWithFlinkContextClassLoader(
            SupplierWithException<T, E> supplier) throws E {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(RpcService.class.getClassLoader())) {
            return supplier.get();
        }
    }
}
