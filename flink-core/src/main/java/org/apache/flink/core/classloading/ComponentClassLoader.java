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

package org.apache.flink.core.classloading;

import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * A {@link URLClassLoader} that restricts which classes can be loaded to those contained within the
 * given classpath, except classes from a given set of packages that are either loaded parent or
 * child-first.
 */
public class ComponentClassLoader extends URLClassLoader {
    private static final ClassLoader PLATFORM_OR_BOOTSTRAP_LOADER;

    private final ClassLoader parentClassLoader;

    private final String[] parentFirstPackages;
    private final String[] childFirstPackages;
    private final String[] parentFirstResourcePrefixes;
    private final String[] childFirstResourcePrefixes;

    public ComponentClassLoader(
            URL[] classpath,
            ClassLoader parentClassLoader,
            String[] parentFirstPackages,
            String[] childFirstPackages) {
        super(classpath, PLATFORM_OR_BOOTSTRAP_LOADER);
        this.parentClassLoader = parentClassLoader;

        this.parentFirstPackages = parentFirstPackages;
        this.childFirstPackages = childFirstPackages;

        parentFirstResourcePrefixes = convertPackagePrefixesToPathPrefixes(parentFirstPackages);
        childFirstResourcePrefixes = convertPackagePrefixesToPathPrefixes(childFirstPackages);
    }

    // ----------------------------------------------------------------------------------------------
    // Class loading
    // ----------------------------------------------------------------------------------------------

    @Override
    protected Class<?> loadClass(final String name, final boolean resolve)
            throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            final Class<?> loadedClass = findLoadedClass(name);
            if (loadedClass != null) {
                return resolveIfNeeded(resolve, loadedClass);
            }

            if (isChildFirstClass(name)) {
                return loadClassFromChildFirst(name, resolve);
            }
            if (isParentFirstClass(name)) {
                return loadClassFromParentFirst(name, resolve);
            }

            // making this behavior configurable (child-only/child-first/parent-first) would allow
            // this class to subsume the
            // FlinkUserCodeClassLoader (with an added exception handler)
            return loadClassFromChildOnly(name, resolve);
        }
    }

    private Class<?> resolveIfNeeded(final boolean resolve, final Class<?> loadedClass) {
        if (resolve) {
            resolveClass(loadedClass);
        }
        return loadedClass;
    }

    private boolean isParentFirstClass(final String name) {
        return Arrays.stream(parentFirstPackages).anyMatch(name::startsWith);
    }

    private boolean isChildFirstClass(final String name) {
        return Arrays.stream(childFirstPackages).anyMatch(name::startsWith);
    }

    private Class<?> loadClassFromChildOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }

    private Class<?> loadClassFromChildFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromChildOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromParentOnly(name, resolve);
        }
    }

    private Class<?> loadClassFromParentOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return resolveIfNeeded(resolve, parentClassLoader.loadClass(name));
    }

    private Class<?> loadClassFromParentFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromParentOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromChildOnly(name, resolve);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Resource loading
    // ----------------------------------------------------------------------------------------------

    @Override
    public URL getResource(final String name) {
        try {
            final Enumeration<URL> resources = getResources(name);
            if (resources.hasMoreElements()) {
                return resources.nextElement();
            }
        } catch (IOException ignored) {
            // mimics the behavior of the JDK
        }
        return null;
    }

    @Override
    public Enumeration<URL> getResources(final String name) throws IOException {
        if (isChildFirstResource(name)) {
            return loadResourceFromChildFirst(name);
        }
        if (isParentFirstResource(name)) {
            return loadResourceFromParentFirst(name);
        }

        return loadResourceFromChildOnly(name);
    }

    private boolean isParentFirstResource(final String name) {
        return Arrays.stream(parentFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    private boolean isChildFirstResource(final String name) {
        return Arrays.stream(childFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    private Enumeration<URL> loadResourceFromChildOnly(final String name) throws IOException {
        return super.getResources(name);
    }

    private Enumeration<URL> loadResourceFromChildFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name, this::loadResourceFromChildOnly, this::loadResourceFromParentOnly);
    }

    private Enumeration<URL> loadResourceFromParentOnly(final String name) throws IOException {
        return parentClassLoader.getResources(name);
    }

    private Enumeration<URL> loadResourceFromParentFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name, this::loadResourceFromParentOnly, this::loadResourceFromChildOnly);
    }

    private interface ResourceLoadingFunction
            extends FunctionWithException<String, Enumeration<URL>, IOException> {}

    private Enumeration<URL> loadResourcesInOrder(
            String name,
            ResourceLoadingFunction firstClassLoader,
            ResourceLoadingFunction secondClassLoader)
            throws IOException {
        final Iterator<URL> iterator =
                Iterators.concat(
                        Iterators.forEnumeration(firstClassLoader.apply(name)),
                        Iterators.forEnumeration(secondClassLoader.apply(name)));

        return new IteratorBackedEnumeration<>(iterator);
    }

    private static class IteratorBackedEnumeration<T> implements Enumeration<T> {
        private final Iterator<T> backingIterator;

        private IteratorBackedEnumeration(Iterator<T> backingIterator) {
            this.backingIterator = backingIterator;
        }

        @Override
        public boolean hasMoreElements() {
            return backingIterator.hasNext();
        }

        @Override
        public T nextElement() {
            return backingIterator.next();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Utils
    // ----------------------------------------------------------------------------------------------

    private static String[] convertPackagePrefixesToPathPrefixes(String[] packagePrefixes) {
        return Arrays.stream(packagePrefixes)
                .map(packageName -> packageName.replace('.', '/'))
                .toArray(String[]::new);
    }

    static {
        ClassLoader platformLoader = null;
        try {
            platformLoader =
                    (ClassLoader)
                            ClassLoader.class.getMethod("getPlatformClassLoader").invoke(null);
        } catch (NoSuchMethodException e) {
            // on Java 8 this method does not exist, but using null indicates the bootstrap
            // loader that we want
            // to have
        } catch (Exception e) {
            throw new IllegalStateException("Cannot retrieve platform classloader on Java 9+", e);
        }
        PLATFORM_OR_BOOTSTRAP_LOADER = platformLoader;
        ClassLoader.registerAsParallelCapable();
    }
}
