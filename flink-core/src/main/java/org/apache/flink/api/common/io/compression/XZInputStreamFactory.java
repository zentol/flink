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
package org.apache.flink.api.common.io.compression;

import org.apache.flink.annotation.Internal;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

@Internal
public class XZInputStreamFactory implements InflaterInputStreamFactory<XZCompressorInputStream> {

	private static XZInputStreamFactory INSTANCE = null;

	public static XZInputStreamFactory getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new XZInputStreamFactory();
		}
		return INSTANCE;
	}

	@Override
	public XZCompressorInputStream create(InputStream in) throws IOException {
		return new XZCompressorInputStream(in, true);
	}

	@Override
	public Collection<String> getCommonFileExtensions() {
		return Collections.singleton("xz");
	}
}
