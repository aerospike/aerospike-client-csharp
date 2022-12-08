/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public sealed class BatchReadListCommand : BatchCommand
	{
		private readonly List<BatchRead> records;

		public BatchReadListCommand
		(
			Cluster cluster,
			Executor parent,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRead> records
		) : base(cluster, parent, batch, policy)
		{
			this.records = records;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, records, batch);
		}

		protected internal override void ParseRow(Key key)
		{
			if (resultCode == 0)
			{
				BatchRead record = records[batchIndex];
				record.record = ParseRecord();
			}
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchReadListCommand(cluster, parent, batchNode, batchPolicy, records);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public sealed class BatchGetArrayCommand : BatchCommand
	{
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Record[] records;
		private readonly int readAttr;

		public BatchGetArrayCommand
		(
			Cluster cluster,
			Executor parent,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			string[] binNames,
			Record[] records,
			int readAttr
		) : base(cluster, parent, batch, policy)
		{
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			if (resultCode == 0)
			{
				records[batchIndex] = ParseRecord();
			}
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchGetArrayCommand(cluster, parent, batchNode, batchPolicy, keys, binNames, records, readAttr);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public sealed class BatchExistsArrayCommand : BatchCommand
	{
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public BatchExistsArrayCommand
		(
			Cluster cluster,
			Executor parent,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			bool[] existsArray
		) : base(cluster, parent, batch, policy)
		{
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[batchIndex] = resultCode == 0;
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchExistsArrayCommand(cluster, parent, batchNode, batchPolicy, keys, existsArray);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch);
		}
	}

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	public abstract class BatchCommand : MultiCommand
	{
		internal readonly Executor parent;
		internal readonly BatchNode batch;
		internal readonly BatchPolicy batchPolicy;
		internal uint sequenceAP;
		internal uint sequenceSC;

		public BatchCommand(Cluster cluster, Executor parent, BatchNode batch, BatchPolicy batchPolicy)
			: base(cluster, batchPolicy, batch.node, LatencyType.BATCH, false)
		{
			this.parent = parent;
			this.batch = batch;
			this.batchPolicy = batchPolicy;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			if (!((batchPolicy.replica == Replica.SEQUENCE || batchPolicy.replica == Replica.PREFER_RACK) &&
				  (parent == null || !parent.IsDone())))
			{
				// Perform regular retry to same node.
				return true;
			}
			sequenceAP++;

			if (!timeout || batchPolicy.readModeSC != ReadModeSC.LINEARIZE)
			{
				sequenceSC++;
			}
			return false;
		}

		protected internal override bool RetryBatch
		(
			Cluster cluster,
			int socketTimeout,
			int totalTimeout,
			DateTime deadline,
			int iteration,
			int commandSentCounter
		)
		{
			// Retry requires keys for this node to be split among other nodes.
			// This is both recursive and exponential.
			List<BatchNode> batchNodes = GenerateBatchNodes();

			if (batchNodes.Count == 1 && batchNodes[0].node == batch.node)
			{
				// Batch node is the same.  Go through normal retry.
				return false;
			}

			// Run batch requests sequentially in same thread.
			foreach (BatchNode batchNode in batchNodes)
			{
				BatchCommand command = CreateCommand(batchNode);
				command.sequenceAP = sequenceAP;
				command.sequenceSC = sequenceSC;
				command.socketTimeout = socketTimeout;
				command.totalTimeout = totalTimeout;
				command.iteration = iteration;
				command.commandSentCounter = commandSentCounter;
				command.deadline = deadline;
				command.ExecuteCommand();
			}
			return true;
		}

		protected internal abstract BatchCommand CreateCommand(BatchNode batchNode);
		protected internal abstract List<BatchNode> GenerateBatchNodes();
	}
}
