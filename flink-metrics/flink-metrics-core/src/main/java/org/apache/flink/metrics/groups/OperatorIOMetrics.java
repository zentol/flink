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

package org.apache.flink.metrics.groups;

import org.apache.flink.metrics.Counter;

/**
 * Provides access to a collection of operator-related metrics.
 */
public interface OperatorIOMetrics {

	/**
	 * Returns the counter for measuring the number of incoming records.
	 *
	 * @return counter for incoming records
	 */
	Counter getNumRecordsInCounter();

	/**
	 * Returns the counter for measuring the number of outgoing records.
	 *
	 * @return counter for outgoing records
	 */
	Counter getNumRecordsOutCounter();

	/**
	 * Returns the counter for measuring the number of locally incoming bytes.
	 *
	 * <p>This counter should only be used if the caller can guarantee that this operator is either not part of a chain
	 * or a head operator.
	 *
	 * @return counter for locally ncoming bytes
	 */
	Counter getNumBytesInLocalCounter();

	/**
	 * Returns the counter for measuring the number of remotely incoming records.
	 *
	 * <p>This counter should only be used if the caller can guarantee that this operator is either not part of a chain
	 * or a head operator.
	 *
	 * @return counter for remotely incoming bytes
	 */
	Counter getNumBytesInRemoteCounter();

	/**
	 * Returns the counter for measuring the number of outgoing bytes.
	 *
	 * <p>This counter should only be used if the caller can guarantee that this operator is either not part of a chain
	 * or a tail operator.
	 *
	 * @return counter for outgoing bytes
	 */
	Counter getNumBytesOutCounter();
}
