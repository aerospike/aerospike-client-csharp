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
	/// <summary>
	/// Batch key and read only operations with default policy.
	/// Used in batch read commands where different bins are needed for each key.
	/// </summary>
	public sealed class BatchRead : BatchRecord
	{
		/// <summary>
		/// Optional read policy.
		/// </summary>
		public readonly BatchReadPolicy policy;

		/// <summary>
		/// Bins to retrieve for this key. binNames are mutually exclusive with
		/// <see cref="Aerospike.Client.BatchRead.ops"/>.
		/// </summary>
		public readonly string[] binNames;

		/// <summary>
		/// Optional operations for this key. ops are mutually exclusive with
		/// <see cref="Aerospike.Client.BatchRead.binNames"/>. A binName can be emulated with
		/// <see cref="Aerospike.Client.Operation.Get(string)"/>
		/// </summary>
		public readonly Operation[] ops;

		/// <summary>
		/// If true, ignore binNames and read all bins.
		/// If false and binNames are set, read specified binNames.
		/// If false and binNames are not set, read record header (generation, expiration) only.
		/// </summary>
		public readonly bool readAllBins;

		/// <summary>
		/// Initialize batch key and bins to retrieve.
		/// </summary>
		public BatchRead(Key key, string[] binNames)
			: base(key, false)
		{
			this.policy = null;
			this.binNames = binNames;
			this.ops = null;
			this.readAllBins = false;
		}

		/// <summary>
		/// Initialize batch key and readAllBins indicator.
		/// </summary>
		public BatchRead(Key key, bool readAllBins)
			: base(key, false)
		{
			this.policy = null;
			this.binNames = null;
			this.ops = null;
			this.readAllBins = readAllBins;
		}

		/// <summary>
		/// Initialize batch key and read operations.
		/// </summary>
		public BatchRead(Key key, Operation[] ops)
			: base(key, false)
		{
			this.policy = null;
			this.binNames = null;
			this.ops = ops;
			this.readAllBins = false;
		}

		/// <summary>
		/// Initialize batch policy, key and bins to retrieve.
		/// </summary>
		public BatchRead(BatchReadPolicy policy, Key key, string[] binNames)
			: base(key, false)
		{
			this.policy = policy;
			this.binNames = binNames;
			this.ops = null;
			this.readAllBins = false;
		}

		/// <summary>
		/// Initialize batch policy, key and readAllBins indicator.
		/// </summary>
		public BatchRead(BatchReadPolicy policy, Key key, bool readAllBins)
			: base(key, false)
		{
			this.policy = policy;
			this.binNames = null;
			this.ops = null;
			this.readAllBins = readAllBins;
		}

		/// <summary>
		/// Initialize batch policy, key and read operations.
		/// </summary>
		public BatchRead(BatchReadPolicy policy, Key key, Operation[] ops)
			: base(key, false)
		{
			this.policy = policy;
			this.binNames = null;
			this.ops = ops;
			this.readAllBins = false;
		}

		/// <summary>
		/// Return batch command type.
		/// </summary>
		public override Type GetBatchType()
		{
			return Type.BATCH_READ;
		}

		/// <summary>
		/// Optimized reference equality check to determine batch wire protocol repeat flag.
		/// For internal use only.
		/// </summary>
		public override bool Equals(BatchRecord obj)
		{
			if (this.GetType() != obj.GetType())
			{
				return false;
			}

			BatchRead other = (BatchRead)obj;
			return binNames == other.binNames && ops == other.ops && policy == other.policy && readAllBins == other.readAllBins;
		}

		/// <summary>
		/// Return wire protocol size. For internal use only.
		/// </summary>
		public override int Size()
		{
			int size = 0;

			if (policy != null)
			{
				if (policy.filterExp != null)
				{
					size += policy.filterExp.Size();
				}
			}

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					size += ByteUtil.EstimateSizeUtf8(binName) + Command.OPERATION_HEADER_SIZE;
				}
			}
			else if (ops != null)
			{
				foreach (Operation op in ops)
				{
					if (Operation.IsWrite(op.type))
					{
						throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
					}
					size += ByteUtil.EstimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
					size += op.value.EstimateSize();
				}
			}
			return size;
		}
	}
}
