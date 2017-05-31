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

package org.apache.flink.optimizer.operators;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Defines the possible global properties for a join.
 */
public abstract class AbstractJoinDescriptor extends OperatorDescriptorDual {

	private final boolean broadcastFirstAllowed;
	private final boolean broadcastSecondAllowed;
	private final boolean repartitionAllowed;

	private Partitioner<?> customPartitioner;

	protected AbstractJoinDescriptor(FieldList keys1, FieldList keys2) {
		this(keys1, keys2, true, true, true);
	}

	protected AbstractJoinDescriptor(FieldList keys1, FieldList keys2,
			boolean broadcastFirstAllowed, boolean broadcastSecondAllowed, boolean repartitionAllowed)
	{
		super(keys1, keys2);

		this.broadcastFirstAllowed = broadcastFirstAllowed;
		this.broadcastSecondAllowed = broadcastSecondAllowed;
		this.repartitionAllowed = repartitionAllowed;
	}

	public void setCustomPartitioner(Partitioner<?> partitioner) {
		customPartitioner = partitioner;
	}

	@Override
	protected List<GlobalPropertiesPair> createPossibleGlobalProperties() {
		ArrayList<GlobalPropertiesPair> pairs = new ArrayList<GlobalPropertiesPair>();

		if (repartitionAllowed) {
			// partition both (hash or custom)
			if (this.customPartitioner == null) {

				// we accept compatible partitionings of any type
				RequestedGlobalProperties partitionedLeftAny = new RequestedGlobalProperties();
				RequestedGlobalProperties partitionedRightAny = new RequestedGlobalProperties();
				partitionedLeftAny.setAnyPartitioning(this.keys1);
				partitionedRightAny.setAnyPartitioning(this.keys2);
				pairs.add(new GlobalPropertiesPair(partitionedLeftAny, partitionedRightAny));

				// add strict hash partitioning of both inputs on their full key sets
				RequestedGlobalProperties partitionedLeftHash = new RequestedGlobalProperties();
				RequestedGlobalProperties partitionedRightHash = new RequestedGlobalProperties();
				partitionedLeftHash.setHashPartitioned(this.keys1);
				partitionedRightHash.setHashPartitioned(this.keys2);
				pairs.add(new GlobalPropertiesPair(partitionedLeftHash, partitionedRightHash));
			}
			else {
				RequestedGlobalProperties partitionedLeft = new RequestedGlobalProperties();
				partitionedLeft.setCustomPartitioned(this.keys1, this.customPartitioner);

				RequestedGlobalProperties partitionedRight = new RequestedGlobalProperties();
				partitionedRight.setCustomPartitioned(this.keys2, this.customPartitioner);

				return Collections.singletonList(new GlobalPropertiesPair(partitionedLeft, partitionedRight));
			}

			RequestedGlobalProperties partitioned1 = new RequestedGlobalProperties();
			if (customPartitioner == null) {
				partitioned1.setAnyPartitioning(this.keys1);
			} else {
				partitioned1.setCustomPartitioned(this.keys1, this.customPartitioner);
			}

			RequestedGlobalProperties partitioned2 = new RequestedGlobalProperties();
			if (customPartitioner == null) {
				partitioned2.setAnyPartitioning(this.keys2);
			} else {
				partitioned2.setCustomPartitioned(this.keys2, this.customPartitioner);
			}

			pairs.add(new GlobalPropertiesPair(partitioned1, partitioned2));
		}

		if (broadcastSecondAllowed) {
			// replicate second
			RequestedGlobalProperties any1 = new RequestedGlobalProperties();
			RequestedGlobalProperties replicated2 = new RequestedGlobalProperties();
			replicated2.setFullyReplicated();
			pairs.add(new GlobalPropertiesPair(any1, replicated2));
		}

		if (broadcastFirstAllowed) {
			// replicate first
			RequestedGlobalProperties replicated1 = new RequestedGlobalProperties();
			replicated1.setFullyReplicated();
			RequestedGlobalProperties any2 = new RequestedGlobalProperties();
			pairs.add(new GlobalPropertiesPair(replicated1, any2));
		}
		return pairs;
	}

	@Override
	public boolean areCompatible(RequestedGlobalProperties requested1, RequestedGlobalProperties requested2,
			GlobalProperties produced1, GlobalProperties produced2)
	{
		if (requested1.getPartitioning().isPartitionedOnKey() && requested2.getPartitioning().isPartitionedOnKey()) {

			if(produced1.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
					produced2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED) {

				// both are hash partitioned, check that partitioning fields are equivalently chosen
				return checkEquivalentFieldPositionsInKeyFields(
						produced1.getPartitioningFields(), produced2.getPartitioningFields());

			}
			else if(produced1.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED &&
					produced2.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED &&
					produced1.getDataDistribution() != null && produced2.getDataDistribution() != null) {

				return produced1.getPartitioningFields().size() == produced2.getPartitioningFields().size() &&
						checkSameOrdering(produced1, produced2, produced1.getPartitioningFields().size()) &&
						produced1.getDataDistribution().equals(produced2.getDataDistribution());

			}
			else if(produced1.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING &&
					produced2.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING) {

				// both use a custom partitioner. Check that both keys are exactly as specified and that both the same partitioner
				return produced1.getPartitioningFields().isExactMatch(this.keys1) &&
						produced2.getPartitioningFields().isExactMatch(this.keys2) &&
						produced1.getCustomPartitioner() != null && produced2.getCustomPartitioner() != null &&
						produced1.getCustomPartitioner().equals(produced2.getCustomPartitioner());

			}
			else {

				// no other partitioning valid, incl. ANY_PARTITIONING.
				//   For joins we must ensure that both sides are exactly identically partitioned, ANY is not good enough.
				return false;
			}

		} else {
			return true;
		}

	}

	@Override
	public GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2) {
		GlobalProperties gp = GlobalProperties.combine(in1, in2);
		if (gp.getUniqueFieldCombination() != null && gp.getUniqueFieldCombination().size() > 0 &&
					gp.getPartitioning() == PartitioningProperty.RANDOM_PARTITIONED)
		{
			gp.setAnyPartitioning(gp.getUniqueFieldCombination().iterator().next().toFieldList());
		}
		gp.clearUniqueFieldCombinations();
		return gp;
	}

}
