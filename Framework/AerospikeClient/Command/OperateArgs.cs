/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
	public sealed class OperateArgs
	{
		public readonly WritePolicy writePolicy;
		public readonly Operation[] operations;
		public readonly Partition partition;
		public readonly int size;
		public readonly int readAttr;
		public readonly int writeAttr;
		public readonly bool hasWrite;

		public OperateArgs
		(
			Cluster cluster,
			WritePolicy policy,
			WritePolicy writeDefault,
			WritePolicy readDefault,
			Key key,
			Operation[] operations
		)
		{
			this.operations = operations;

			int dataOffset = 0;
			int rattr = 0;
			int wattr = 0;
			bool write = false;
			bool readBin = false;
			bool readHeader = false;
			bool respondAllOps = false;

			foreach (Operation operation in operations)
			{
				switch (operation.type)
				{
					case Operation.Type.BIT_READ:
					case Operation.Type.MAP_READ:
						// Map operations require respondAllOps to be true.
						respondAllOps = true;
						rattr |= Command.INFO1_READ;

						// Read all bins if no bin is specified.
						if (operation.binName == null)
						{
							rattr |= Command.INFO1_GET_ALL;
						}
						readBin = true;
						break;
					
					case Operation.Type.CDT_READ:
					case Operation.Type.READ:
						rattr |= Command.INFO1_READ;

						// Read all bins if no bin is specified.
						if (operation.binName == null)
						{
							rattr |= Command.INFO1_GET_ALL;
						}
						readBin = true;
						break;

					case Operation.Type.READ_HEADER:
						rattr |= Command.INFO1_READ;
						readHeader = true;
						break;

					case Operation.Type.BIT_MODIFY:
					case Operation.Type.MAP_MODIFY:
						// Map operations require respondAllOps to be true.
						respondAllOps = true;
						wattr = Command.INFO2_WRITE;
						write = true;
						break;

					default:
						wattr = Command.INFO2_WRITE;
						write = true;
						break;
				}
				dataOffset += ByteUtil.EstimateSizeUtf8(operation.binName) + Command.OPERATION_HEADER_SIZE;
				dataOffset += operation.value.EstimateSize();
			}
			size = dataOffset;
			hasWrite = write;

			if (readHeader && !readBin)
			{
				rattr |= Command.INFO1_NOBINDATA;
			}
			readAttr = rattr;

			if (policy == null)
			{
				if (write)
				{
					writePolicy = writeDefault;
				}
				else
				{
					writePolicy = readDefault;
				}
			}
			else
			{
				writePolicy = policy;
			}

			if (respondAllOps || writePolicy.respondAllOps)
			{
				wattr |= Command.INFO2_RESPOND_ALL_OPS;
			}
			writeAttr = wattr;

			if (write)
			{
				partition = Partition.Write(cluster, writePolicy, key);
			}
			else
			{
				partition = Partition.Read(cluster, writePolicy, key);
			}
		}
	}
}
