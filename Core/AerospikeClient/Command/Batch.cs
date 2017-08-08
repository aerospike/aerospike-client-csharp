/* 
 * Copyright 2012-2017 Aerospike, Inc.
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

	public sealed class BatchReadListCommand : MultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly List<BatchRead> records;

		public BatchReadListCommand(BatchNode batch, BatchPolicy policy, List<BatchRead> records)
			: base(false)
		{
			this.batch = batch;
			this.policy = policy;
			this.records = records;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, records, batch);
		}

		protected internal override void ParseRow(Key key)
		{
			BatchRead record = records[batchIndex];

			if (Util.ByteArrayEquals(key.digest, record.key.digest))
			{
				if (resultCode == 0)
				{
					record.record = ParseRecord();
				}
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------
	
	public sealed class BatchGetArrayCommand : MultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Record[] records;
		private readonly int readAttr;

		public BatchGetArrayCommand
		(
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			string[] binNames,
			Record[] records,
			int readAttr
		) : base(false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			if (Util.ByteArrayEquals(key.digest, keys[batchIndex].digest))
			{
				if (resultCode == 0)
				{
					records[batchIndex] = ParseRecord();
				}
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}
	}

	public sealed class BatchGetArrayDirect : MultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Record[] records;
		private readonly int readAttr;
		private int index;

		public BatchGetArrayDirect
		(
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			string[] binNames,
			Record[] records,
			int readAttr
		) : base(false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchReadDirect(policy, keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			int offset = batch.offsets[index++];

			if (Util.ByteArrayEquals(key.digest, keys[offset].digest))
			{
				if (resultCode == 0)
				{
					records[offset] = ParseRecord();
				}
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------
	
	public sealed class BatchExistsArrayCommand : MultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public BatchExistsArrayCommand
		(
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			bool[] existsArray
		) : base(false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			if (Util.ByteArrayEquals(key.digest, keys[batchIndex].digest))
			{
				existsArray[batchIndex] = resultCode == 0;
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}
	}

	public sealed class BatchExistsArrayDirect : MultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly bool[] existsArray;
		private int index;

		public BatchExistsArrayDirect
		(
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			bool[] existsArray
		) : base(false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchReadDirect(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			int offset = batch.offsets[index++];

			if (Util.ByteArrayEquals(key.digest, keys[offset].digest))
			{
				existsArray[offset] = resultCode == 0;
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
		}
	}
}
