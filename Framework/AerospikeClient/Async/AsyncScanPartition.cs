/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
namespace Aerospike.Client
{
	public sealed class AsyncScanPartition : AsyncMultiCommand
	{
		private readonly ScanPolicy scanPolicy;
		private readonly RecordSequenceListener listener;
		private readonly string ns;
		private readonly string setName;
		private readonly string[] binNames;
		private readonly ulong taskId;
		private readonly PartitionTracker tracker;
		private readonly NodePartitions nodePartitions;

		public AsyncScanPartition
		(
			AsyncMultiExecutor executor,
			AsyncCluster cluster,
			ScanPolicy scanPolicy,
			RecordSequenceListener listener,
			string ns,
			string setName,
			string[] binNames,
			ulong taskId,
			PartitionTracker tracker,
			NodePartitions nodePartitions
		) : base(executor, cluster, scanPolicy, (AsyncNode)nodePartitions.node, tracker.socketTimeout, tracker.totalTimeout)
		{
			this.scanPolicy = scanPolicy;
			this.listener = listener;
			this.ns = ns;
			this.setName = setName;
			this.binNames = binNames;
			this.taskId = taskId;
			this.tracker = tracker;
			this.nodePartitions = nodePartitions;
		}

		protected internal override void WriteBuffer()
		{
			SetScan(scanPolicy, ns, setName, binNames, taskId, nodePartitions);
		}

		protected internal override void ParseRow(Key key)
		{
			if ((info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				tracker.PartitionDone(nodePartitions, generation);
				return;
			}

			Record record = ParseRecord();
			tracker.SetDigest(nodePartitions, key);
			listener.OnRecord(key, record);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			if (tracker.ShouldRetry(ae))
			{
				executor.ChildSuccess(serverNode);
				return;
			}
			executor.ChildFailure(ae);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return null;
		}
	}
}
