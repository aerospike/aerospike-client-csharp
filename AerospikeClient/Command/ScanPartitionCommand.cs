/* 
 * Copyright 2012-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System;

namespace Aerospike.Client
{
	public sealed class ScanPartitionCommand : MultiCommand
	{
		private readonly ScanPolicy scanPolicy;
		private readonly string setName;
		private readonly string[] binNames;
		private readonly ScanCallback callback;
		private readonly ulong taskId;
		private readonly PartitionTracker tracker;
		private readonly NodePartitions nodePartitions;

		public ScanPartitionCommand
		(
			Cluster cluster,
			ScanPolicy scanPolicy,
			string ns,
			string setName,
			string[] binNames,
			ScanCallback callback,
			ulong taskId,
			PartitionTracker tracker,
			NodePartitions nodePartitions
		) : base(cluster, scanPolicy, nodePartitions.node, ns, tracker.socketTimeout, tracker.totalTimeout)
		{
			this.scanPolicy = scanPolicy;
			this.setName = setName;
			this.binNames = binNames;
			this.callback = callback;
			this.taskId = taskId;
			this.tracker = tracker;
			this.nodePartitions = nodePartitions;
		}

		public override void Execute()
		{
			try
			{
				ExecuteCommand();
			}
			catch (AerospikeException ae)
			{
				if (!tracker.ShouldRetry(nodePartitions, ae))
				{
					throw ae;
				}
			}
		}

		protected internal override void WriteBuffer()
		{
			SetScan(cluster, scanPolicy, ns, setName, binNames, taskId, nodePartitions);
		}

		protected internal override bool ParseRow()
		{
			ulong bval;
			Key key = ParseKey(fieldCount, out bval);

			if ((info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partitionId.
				if (resultCode != 0)
				{
					tracker.PartitionUnavailable(nodePartitions, generation);
				}
				return true;
			}

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}

			Record record = ParseRecord();

			if (!valid)
			{
				throw new AerospikeException.ScanTerminated();
			}

			if (tracker.AllowRecord())
			{
				callback(key, record);
				tracker.SetDigest(nodePartitions, key);
			}
			return true;
		}
	}
}
