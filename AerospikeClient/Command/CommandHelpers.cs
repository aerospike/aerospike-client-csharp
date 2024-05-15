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
using System.Buffers;
using System.Collections;

namespace Aerospike.Client
{
	public static class CommandHelpers
	{
		public static readonly int INFO1_READ = (1 << 0); // Contains a read operation.
		public static readonly int INFO1_GET_ALL = (1 << 1); // Get all bins.
		public static readonly int INFO1_SHORT_QUERY = (1 << 2); // Short query.
		public static readonly int INFO1_BATCH = (1 << 3); // Batch read or exists.
		public static readonly int INFO1_NOBINDATA = (1 << 5); // Do not read the bins.
		public static readonly int INFO1_READ_MODE_AP_ALL = (1 << 6); // Involve all replicas in read operation.
		public static readonly int INFO1_COMPRESS_RESPONSE = (1 << 7); // Tell server to compress it's response.

		public static readonly int INFO2_WRITE = (1 << 0); // Create or update record
		public static readonly int INFO2_DELETE = (1 << 1); // Fling a record into the belly of Moloch.
		public static readonly int INFO2_GENERATION = (1 << 2); // Update if expected generation == old.
		public static readonly int INFO2_GENERATION_GT = (1 << 3); // Update if new generation >= old, good for restore.
		public static readonly int INFO2_DURABLE_DELETE = (1 << 4); // Transaction resulting in record deletion leaves tombstone (Enterprise only).
		public static readonly int INFO2_CREATE_ONLY = (1 << 5); // Create only. Fail if record already exists.
		public static readonly int INFO2_RELAX_AP_LONG_QUERY = (1 << 6); // Treat as long query, but relac read consistency
		public static readonly int INFO2_RESPOND_ALL_OPS = (1 << 7); // Return a result for every operation.

		public static readonly int INFO3_LAST = (1 << 0); // This is the last of a multi-part message.
		public static readonly int INFO3_COMMIT_MASTER = (1 << 1); // Commit to master only before declaring success.
																   // On send: Do not return partition done in scan/query.
																   // On receive: Specified partition is done in scan/query.
		public static readonly int INFO3_PARTITION_DONE = (1 << 2);
		public static readonly int INFO3_UPDATE_ONLY = (1 << 3); // Update only. Merge bins.
		public static readonly int INFO3_CREATE_OR_REPLACE = (1 << 4); // Create or completely replace record.
		public static readonly int INFO3_REPLACE_ONLY = (1 << 5); // Completely replace existing record only.
		public static readonly int INFO3_SC_READ_TYPE = (1 << 6); // See below.
		public static readonly int INFO3_SC_READ_RELAX = (1 << 7); // See below.

		// Interpret SC_READ bits in info3.
		//
		// RELAX   TYPE
		//	              strict
		//	              ------
		//   0      0     sequential (default)
		//   0      1     linearize
		//
		//	              relaxed
		//	              -------
		//   1      0     allow replica
		//   1      1     allow unavailable

		public const byte BATCH_MSG_READ = 0x0;
		public const byte BATCH_MSG_REPEAT = 0x1;
		public const byte BATCH_MSG_INFO = 0x2;
		public const byte BATCH_MSG_GEN = 0x4;
		public const byte BATCH_MSG_TTL = 0x8;

		public const int MSG_TOTAL_HEADER_SIZE = 30;
		public const int FIELD_HEADER_SIZE = 5;
		public const int OPERATION_HEADER_SIZE = 8;
		public const int MSG_REMAINING_HEADER_SIZE = 22;
		public const int DIGEST_SIZE = 20;
		public const int COMPRESS_THRESHOLD = 128;
		public const ulong CL_MSG_VERSION = 2UL;
		public const ulong AS_MSG_TYPE = 3UL;
		public const ulong MSG_TYPE_COMPRESSED = 4UL;

		//--------------------------------------------------
		// Writes
		//--------------------------------------------------

		public static void SetWrite(this CommandNew command, byte[] dataBuffer, ref int dataOffset, WritePolicy policy, Operation.Type operation, Key key, Bin[] bins)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			foreach (Bin bin in bins)
			{
				EstimateOperationSize(ref dataOffset, bin);
			}
			
			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			WriteHeaderWrite(command, dataBuffer, ref dataOffset, policy, INFO2_WRITE, fieldCount, bins.Length);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);

			foreach (Bin bin in bins)
			{
				WriteOperation(dataBuffer, ref dataOffset, bin, operation);
			}
			End(command, dataBuffer, ref dataOffset, compress);
		}

		public static void SetDelete(this CommandNew command, byte[] dataBuffer, ref int dataOffset, WritePolicy policy, Key key)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.SizeBuffer();
			WriteHeaderWrite(command, dataBuffer, ref dataOffset, policy, INFO2_WRITE | INFO2_DELETE, fieldCount, 0);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);
			command.End(dataBuffer, ref dataOffset);
		}

		public static void SetTouch(this CommandNew command, byte[] dataBuffer, ref int dataOffset, WritePolicy policy, Key key)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			EstimateOperationSize(ref dataOffset);
			command.SizeBuffer();
			WriteHeaderWrite(command, dataBuffer, ref dataOffset, policy, INFO2_WRITE, fieldCount, 1);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);
			WriteOperation(dataBuffer, ref dataOffset, Operation.Type.TOUCH);
			command.End(dataBuffer, ref dataOffset);
		}

		//--------------------------------------------------
		// Reads
		//--------------------------------------------------

		public static void SetExists(this CommandNew command, byte[] dataBuffer, ref int dataOffset, Policy policy, Key key)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.SizeBuffer();
			WriteHeaderReadHeader(command, dataBuffer, ref dataOffset, policy, INFO1_READ | INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);
			command.End(dataBuffer, ref dataOffset);
		}

		public static void SetRead(this CommandNew command, byte[] dataBuffer, ref int dataOffset, Policy policy, Key key)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.SizeBuffer();
			WriteHeaderRead(dataBuffer, ref dataOffset, policy, command.ServerTimeout, INFO1_READ | INFO1_GET_ALL, 0, 0, fieldCount, 0);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);
			command.End(dataBuffer, ref dataOffset);
		}

		public static void SetRead(this CommandNew command, byte[] dataBuffer, ref int dataOffset, Policy policy, Key key, string[] binNames)
		{
			if (binNames != null)
			{
				Begin(ref dataOffset);
				int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

				if (policy.filterExp != null)
				{
					dataOffset += policy.filterExp.Size();
					fieldCount++;
				}

				foreach (string binName in binNames)
				{
					EstimateOperationSize(ref dataOffset, binName);
				}
				command.SizeBuffer();
				WriteHeaderRead(dataBuffer, ref dataOffset, policy, command.ServerTimeout, INFO1_READ, 0, 0, fieldCount, binNames.Length);
				WriteKey(dataBuffer, ref dataOffset, policy, key);

				policy.filterExp?.Write(command);

				foreach (string binName in binNames)
				{
					WriteOperation(dataBuffer, ref dataOffset, binName, Operation.Type.READ);
				}
				command.End(dataBuffer, ref dataOffset);
			}
			else
			{
				SetRead(command, dataBuffer, ref dataOffset, policy, key);
			}
		}

		public static void SetReadHeader(this CommandNew command, byte[] dataBuffer, ref int dataOffset, Policy policy, Key key)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			EstimateOperationSize(ref dataOffset, (string)null);
			command.SizeBuffer();
			WriteHeaderReadHeader(command, dataBuffer, ref dataOffset, policy, INFO1_READ |	INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);
			command.End(dataBuffer, ref dataOffset);
		}

		//--------------------------------------------------
		// Operate
		//--------------------------------------------------

		public static void SetOperate(this CommandNew command, byte[] dataBuffer, ref int dataOffset, WritePolicy policy, Key key, OperateArgs args)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			dataOffset += args.size;

			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			WriteHeaderReadWrite(command, dataBuffer, ref dataOffset, policy, args, fieldCount);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);

			foreach (Operation operation in args.operations)
			{
				WriteOperation(dataBuffer, ref dataOffset, operation);
			}
			End(command, dataBuffer, ref dataOffset, compress);
		}

		//--------------------------------------------------
		// UDF
		//--------------------------------------------------

		public static void SetUdf(this CommandNew command, byte[] dataBuffer, ref int dataOffset, WritePolicy policy, Key key, string packageName, string functionName, Value[] args)
		{
			Begin(ref dataOffset);
			int fieldCount = EstimateKeySize(ref dataOffset, policy, key);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			byte[] argBytes = Packer.Pack(args);
			fieldCount += EstimateUdfSize(ref dataOffset, packageName, functionName, argBytes);

			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			WriteHeaderWrite(command, dataBuffer, ref dataOffset, policy, INFO2_WRITE, fieldCount, 0);
			WriteKey(dataBuffer, ref dataOffset, policy, key);

			policy.filterExp?.Write(command);
			WriteField(dataBuffer, ref dataOffset, packageName, FieldType.UDF_PACKAGE_NAME);
			WriteField(dataBuffer, ref dataOffset, functionName, FieldType.UDF_FUNCTION);
			WriteField(dataBuffer, ref dataOffset, argBytes, FieldType.UDF_ARGLIST);
			End(command, dataBuffer, ref dataOffset, compress);
		}

		//--------------------------------------------------
		// Batch Read Only
		//--------------------------------------------------

		public static void SetBatchRead(this CommandNew command, byte[] dataBuffer, ref int dataOffset, BatchPolicy policy, List<BatchRead> records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			BatchRead prev = null;

			Begin(ref dataOffset);
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			for (int i = 0; i < max; i++)
			{
				BatchRead record = records[offsets[i]];
				Key key = record.key;
				string[] binNames = record.binNames;
				Operation[] ops = record.ops;

				dataOffset += key.digest.Length + 4;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
					prev.ops == ops)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							EstimateOperationSize(ref dataOffset, binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							EstimateReadOperationSize(ref dataOffset, op);
						}
					}
					prev = record;
				}
			}

			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			int readAttr = INFO1_READ;

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			WriteHeaderRead(dataBuffer, ref dataOffset, policy, command.TotalTimeout, readAttr | INFO1_BATCH, 0, 0, fieldCount, 0);

			policy.filterExp?.Write(command);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(dataBuffer, ref dataOffset, 0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = (policy.allowInline) ? (byte)1 : (byte)0;
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, dataBuffer, dataOffset);
				dataOffset += 4;

				BatchRead record = records[index];
				Key key = record.key;
				string[] binNames = record.binNames;
				Operation[] ops = record.ops;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.		
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
					prev.ops == ops)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full header, namespace and bin names.
					dataBuffer[dataOffset++] = BATCH_MSG_READ;

					if (binNames != null && binNames.Length != 0)
					{
						dataBuffer[dataOffset++] = (byte)readAttr;
						WriteBatchFields(dataBuffer, ref dataOffset, key, 0, binNames.Length);

						foreach (string binName in binNames)
						{
							WriteOperation(dataBuffer, ref dataOffset, binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = dataOffset++;
						WriteBatchFields(dataBuffer, ref dataOffset, key, 0, ops.Length);
						dataBuffer[offset] = (byte)WriteReadOnlyOperations(dataBuffer, ref dataOffset, ops, readAttr);
					}
					else
					{
						dataBuffer[dataOffset++] = (byte)(readAttr | (record.readAllBins ? INFO1_GET_ALL : INFO1_NOBINDATA));
						WriteBatchFields(dataBuffer, ref dataOffset, key, 0, 0);
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(command, dataBuffer, ref dataOffset, compress);
		}

		public static void SetBatchRead
		(
			this CommandNew command,
			byte[] dataBuffer, 
			ref int dataOffset, 
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string[] binNames,
			Operation[] ops,
			int readAttr
		)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			// Estimate dataBuffer size.
			Begin(ref dataOffset);
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			dataOffset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				dataOffset += key.digest.Length + 4;

				// Try reference equality in hope that namespace for all keys is set from a fixed variable.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName) 
				{	
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (binNames != null)
					{
						foreach (String binName in binNames)
						{
							EstimateOperationSize(ref dataOffset, binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							EstimateReadOperationSize(ref dataOffset, op);
						}
					}
					prev = key;
				}
			}

			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			WriteHeaderRead(dataBuffer, ref dataOffset, policy, command.TotalTimeout, readAttr | INFO1_BATCH, 0, 0, fieldCount, 0);

			policy.filterExp?.Write(command);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(dataBuffer, ref dataOffset, 0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = (policy.allowInline) ? (byte)1 : (byte)0;
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, dataBuffer, dataOffset);
				dataOffset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				// Try reference equality in hope that namespace for all keys is set from a fixed variable.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full header, namespace and bin names.
					dataBuffer[dataOffset++] = BATCH_MSG_READ;

					if (binNames != null && binNames.Length != 0)
					{
						dataBuffer[dataOffset++] = (byte)readAttr;
						WriteBatchFields(dataBuffer, ref dataOffset, key, 0, binNames.Length);

						foreach (String binName in binNames)
						{
							WriteOperation(dataBuffer, ref dataOffset, binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = dataOffset++;
						WriteBatchFields(dataBuffer, ref dataOffset, key, 0, ops.Length);
						dataBuffer[offset] = (byte)WriteReadOnlyOperations(dataBuffer, ref dataOffset, ops, readAttr);
					}
					else
					{
						dataBuffer[dataOffset++] = (byte)readAttr;
						WriteBatchFields(dataBuffer, ref dataOffset, key, 0, 0);
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(command, dataBuffer, ref dataOffset, compress);
		}

		//--------------------------------------------------
		// Batch Read/Write Operations
		//--------------------------------------------------

		public static void SetBatchOperate(this CommandNew command, byte[] dataBuffer, ref int dataOffset, BatchPolicy policy, IList records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			BatchRecord prev = null;

			Begin(ref dataOffset);
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			for (int i = 0; i < max; i++)
			{
				BatchRecord record = (BatchRecord)records[offsets[i]];
				Key key = record.key;

				dataOffset += key.digest.Length + 4;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from
				// fixed variables.  It's fine if equality not determined correctly because it just
				// results in more space used. The batch will still be correct.
				if (!policy.sendKey && prev != null && prev.key.ns == key.ns && 
					prev.key.setName == key.setName && record.Equals(prev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					dataOffset += 12;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					dataOffset += record.Size(policy);
					prev = record;
				}
			}

			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			WriteBatchHeader(dataBuffer, ref dataOffset, policy, command.TotalTimeout, fieldCount);

			policy.filterExp?.Write(command);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(dataBuffer, ref dataOffset, 0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);

			BatchAttr attr = new();
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, dataBuffer, dataOffset);
				dataOffset += 4;

				BatchRecord record = (BatchRecord)records[index];
				Key key = record.key;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from
				// fixed variables.  It's fine if equality not determined correctly because it just
				// results in more space used. The batch will still be correct.
				if (!policy.sendKey && prev != null && prev.key.ns == key.ns &&
					prev.key.setName == key.setName && record.Equals(prev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					switch (record.GetBatchType())
					{
						case BatchRecord.Type.BATCH_READ:
						{
							BatchRead br = (BatchRead)record;

							if (br.policy != null)
							{
								attr.SetRead(br.policy);
							}
							else
							{
								attr.SetRead(policy);
							}

							if (br.binNames != null)
							{
								WriteBatchBinNames(command, dataBuffer, ref dataOffset, key, br.binNames, attr, attr.filterExp);
							}
							else if (br.ops != null)
							{
								attr.AdjustRead(br.ops);
								WriteBatchOperations(command, dataBuffer, ref dataOffset, key, br.ops, attr, attr.filterExp);
							}
							else
							{
								attr.AdjustRead(br.readAllBins);
								WriteBatchRead(command,dataBuffer, ref dataOffset, key, attr, attr.filterExp, 0);
							}
							break;
						}

						case BatchRecord.Type.BATCH_WRITE:
						{
							BatchWrite bw = (BatchWrite)record;

							if (bw.policy != null)
							{
								attr.SetWrite(bw.policy);
							}
							else
							{
								attr.SetWrite(policy);
							}
							attr.AdjustWrite(bw.ops);
							WriteBatchOperations(command, dataBuffer, ref dataOffset, key, bw.ops, attr, attr.filterExp);
							break;
						}

						case BatchRecord.Type.BATCH_UDF:
						{
							BatchUDF bu = (BatchUDF)record;

							if (bu.policy != null)
							{
								attr.SetUDF(bu.policy);
							}
							else
							{
								attr.SetUDF(policy);
							}
							WriteBatchWrite(command, dataBuffer, ref dataOffset, key, attr, attr.filterExp, 3, 0);
							WriteField(dataBuffer, ref dataOffset, bu.packageName, FieldType.UDF_PACKAGE_NAME);
							WriteField(dataBuffer, ref dataOffset, bu.functionName, FieldType.UDF_FUNCTION);
							WriteField(dataBuffer, ref dataOffset, bu.argBytes, FieldType.UDF_ARGLIST);
							break;
						}

						case BatchRecord.Type.BATCH_DELETE:
						{
							BatchDelete bd = (BatchDelete)record;

							if (bd.policy != null)
							{
								attr.SetDelete(bd.policy);
							}
							else
							{
								attr.SetDelete(policy);
							}
							WriteBatchWrite(command, dataBuffer, ref dataOffset, key, attr, attr.filterExp, 0, 0);
							break;
						}
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(command, dataBuffer, ref dataOffset, compress);
		}

		public static void SetBatchOperate
		(
			this CommandNew command,
			byte[] dataBuffer, 
			ref int dataOffset,
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string[] binNames,
			Operation[] ops,
			BatchAttr attr
		)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			// Estimate dataBuffer size.
			Begin(ref dataOffset);
			int fieldCount = 1;
			Expression exp = GetBatchExpression(policy, attr);

			if (exp != null)
			{
				dataOffset += exp.Size();
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				dataOffset += key.digest.Length + 4;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					dataOffset += 12; // header(4) + ttl(4) + fielCount(2) + opCount(2) = 12
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (attr.sendKey)
					{
						dataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							EstimateOperationSize(ref dataOffset, binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							if (Operation.IsWrite(op.type))
							{
								if (!attr.hasWrite)
								{
									throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
								}
								dataOffset += 2; // Extra write specific fields.
							}
							EstimateOperationSize(ref dataOffset, op);
						}
					}
					else if ((attr.writeAttr & INFO2_DELETE) != 0)
					{
						dataOffset += 2; // Extra write specific fields.
					}
					prev = key;
				}
			}

			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			WriteBatchHeader(dataBuffer, ref dataOffset, policy, command.TotalTimeout, fieldCount);

			exp?.Write(command);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(dataBuffer, ref dataOffset, 0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, dataBuffer, dataOffset);
				dataOffset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					if (binNames != null)
					{
						WriteBatchBinNames(command, dataBuffer, ref dataOffset, key, binNames, attr, null);
					}
					else if (ops != null)
					{
						WriteBatchOperations(command, dataBuffer, ref dataOffset, key, ops, attr, null);
					}
					else if ((attr.writeAttr & INFO2_DELETE) != 0)
					{
						WriteBatchWrite(command, dataBuffer, ref dataOffset, key, attr, null, 0, 0);
					}
					else
					{
						WriteBatchRead(command, dataBuffer, ref dataOffset, key, attr, null, 0);
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(command, dataBuffer, ref dataOffset, compress);
		}

		public static void SetBatchUDF
		(
			this CommandNew command,
			byte[] dataBuffer, 
			ref int dataOffset,
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchAttr attr
		)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			// Estimate dataBuffer size.
			Begin(ref dataOffset);
			int fieldCount = 1;
			Expression exp = GetBatchExpression(policy, attr);

			if (exp != null)
			{
				dataOffset += exp.Size();
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				dataOffset += key.digest.Length + 4;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					dataOffset += 12; // header(4) + ttl(4) + fielCount(2) + opCount(2) = 12
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (attr.sendKey)
					{
						dataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}
					dataOffset += 2; // gen(2) = 6
					EstimateUdfSize(ref dataOffset, packageName, functionName, argBytes);
					prev = key;
				}
			}

			bool compress = SizeBuffer(command, ref dataBuffer, ref dataOffset, policy);

			WriteBatchHeader(dataBuffer, ref dataOffset, policy, command.TotalTimeout, fieldCount);

			exp?.Write(command);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(dataBuffer, ref dataOffset, 0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, dataBuffer, dataOffset);
				dataOffset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					WriteBatchWrite(command, dataBuffer, ref dataOffset, key, attr, null, 3, 0);
					WriteField(dataBuffer, ref dataOffset, packageName, FieldType.UDF_PACKAGE_NAME);
					WriteField(dataBuffer, ref dataOffset, functionName, FieldType.UDF_FUNCTION);
					WriteField(dataBuffer, ref dataOffset, argBytes, FieldType.UDF_ARGLIST);
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(command, dataBuffer, ref dataOffset, compress);
		}

		private static Expression GetBatchExpression(Policy policy, BatchAttr attr)
		{
			return attr.filterExp ?? policy.filterExp;
		}

		private static byte GetBatchFlags(BatchPolicy policy)
		{
			byte flags = 0x8;

			if (policy.allowInline)
			{
				flags |= 0x1;
			}

			if (policy.allowInlineSSD)
			{
				flags |= 0x2;
			}

			if (policy.respondAllKeys)
			{
				flags |= 0x4;
			}
			return flags;
		}

		private static void WriteBatchHeader(byte[] dataBuffer, ref int dataOffset, Policy policy, int timeout, int fieldCount)
		{
			int readAttr = INFO1_BATCH;

			if (policy.compress)
			{
				readAttr |= INFO1_COMPRESS_RESPONSE;
			}

			// Write all header data except total size which must be written last.
			dataOffset += 8;
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)readAttr;

			Array.Clear(dataBuffer, dataOffset, 12);
			dataOffset += 12;

			dataOffset += ByteUtil.IntToBytes((uint)timeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes(0, dataBuffer, dataOffset);
		}

		private static void WriteBatchBinNames(CommandNew command, byte[] dataBuffer, ref int dataOffset, Key key, string[] binNames, BatchAttr attr, Expression filter)
		{
			WriteBatchRead(command, dataBuffer, ref dataOffset, key, attr, filter, binNames.Length);

			foreach (string binName in binNames)
			{
				WriteOperation(dataBuffer, ref dataOffset, binName, Operation.Type.READ);
			}
		}

		private static void WriteBatchOperations(CommandNew command, byte[] dataBuffer, ref int dataOffset, Key key, Operation[] ops, BatchAttr attr, Expression filter)
		{
			if (attr.hasWrite)
			{
				WriteBatchWrite(command, dataBuffer, ref dataOffset, key, attr, filter, 0, ops.Length);
			}
			else
			{
				WriteBatchRead(command, dataBuffer, ref dataOffset, key, attr, filter, ops.Length);
			}

			foreach (Operation op in ops)
			{
				WriteOperation(dataBuffer, ref dataOffset, op);
			}
		}

		private static void WriteBatchRead(CommandNew command, byte[] dataBuffer, ref int dataOffset, Key key, BatchAttr attr, Expression filter, int opCount)
		{
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			dataOffset += ByteUtil.IntToBytes((uint)attr.expiration, dataBuffer, dataOffset);
			WriteBatchFields(command, dataBuffer, ref dataOffset, key, filter, 0, opCount);
		}

		private static void WriteBatchWrite(CommandNew command, byte[] dataBuffer, ref int dataOffset, Key key, BatchAttr attr, Expression filter, int fieldCount, int opCount)
		{
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_GEN | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			dataOffset += ByteUtil.ShortToBytes((ushort)attr.generation, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)attr.expiration, dataBuffer, dataOffset);

			if (attr.sendKey)
			{
				fieldCount++;
				WriteBatchFields(command, dataBuffer, ref dataOffset, key, filter, fieldCount, opCount);
				WriteField(dataBuffer, ref dataOffset, key.userKey, FieldType.KEY);
			}
			else
			{
				WriteBatchFields(command, dataBuffer, ref dataOffset, key, filter, fieldCount, opCount);
			}
		}

		private static void WriteBatchFields(CommandNew command, byte[] dataBuffer, ref int dataOffset, Key key, Expression filter, int fieldCount, int opCount)
		{
			if (filter != null)
			{
				fieldCount++;
				WriteBatchFields(dataBuffer, ref dataOffset, key, fieldCount, opCount);
				filter.Write(command);
			}
			else
			{
				WriteBatchFields(dataBuffer, ref dataOffset, key, fieldCount, opCount);
			}
		}

		private static void WriteBatchFields(byte[] dataBuffer, ref int dataOffset, Key key, int fieldCount, int opCount)
		{
			fieldCount += 2;
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)opCount, dataBuffer, dataOffset);
			WriteField(dataBuffer, ref dataOffset, key.ns, FieldType.NAMESPACE);
			WriteField(dataBuffer, ref dataOffset, key.setName, FieldType.TABLE);
		}

		//--------------------------------------------------
		// Scan
		//--------------------------------------------------

		public static void SetScan
		(
			this CommandNew command,
			byte[] dataBuffer, 
			ref int dataOffset,
			Cluster cluster,
			ScanPolicy policy,
			string ns,
			string setName,
			string[] binNames,
			ulong taskId,
			NodePartitions nodePartitions
		)
		{
			Begin(ref dataOffset);
			int fieldCount = 0;
			int partsFullSize = nodePartitions.partsFull.Count * 2;
			int partsPartialSize = nodePartitions.partsPartial.Count * 20;
			long maxRecords = nodePartitions.recordMax;

			if (ns != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (setName != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsFullSize > 0)
			{
				dataOffset += partsFullSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsPartialSize > 0)
			{
				dataOffset += partsPartialSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (maxRecords > 0)
			{
				dataOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (policy.recordsPerSecond > 0)
			{
				dataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			// Estimate scan timeout size.
			dataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate taskId size.
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					EstimateOperationSize(ref dataOffset, binName);
				}
			}

			command.SizeBuffer();
			int readAttr = INFO1_READ;

			if (!policy.includeBinData)
			{
				readAttr |=	INFO1_NOBINDATA;
			}

			// Clusters that support partition queries also support not sending partition done messages.
			int infoAttr = cluster.hasPartitionQuery ? INFO3_PARTITION_DONE : 0;
			int operationCount = (binNames == null) ? 0 : binNames.Length;
			WriteHeaderRead(dataBuffer, ref dataOffset, policy, command.TotalTimeout, readAttr, 0, infoAttr, fieldCount, operationCount);

			if (ns != null)
			{
				WriteField(dataBuffer, ref dataOffset, ns, FieldType.NAMESPACE);
			}

			if (setName != null)
			{
				WriteField(dataBuffer, ref dataOffset, setName, FieldType.TABLE);
			}

			if (partsFullSize > 0)
			{
				WriteFieldHeader(dataBuffer, ref dataOffset, partsFullSize, FieldType.PID_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsFull)
				{
					ByteUtil.ShortToLittleBytes((ushort)part.id, dataBuffer, dataOffset);
					dataOffset += 2;
				}
			}

			if (partsPartialSize > 0)
			{
				WriteFieldHeader(dataBuffer, ref dataOffset, partsPartialSize, FieldType.DIGEST_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial) {
					Array.Copy(part.digest, 0, dataBuffer, dataOffset, 20); 
					dataOffset += 20;
				}
			}

			if (maxRecords > 0)
			{
				WriteField(dataBuffer, ref dataOffset, (ulong)maxRecords, FieldType.MAX_RECORDS);
			}

			if (policy.recordsPerSecond > 0)
			{
				WriteField(dataBuffer, ref dataOffset, policy.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			policy.filterExp?.Write(command);

			// Write scan timeout
			WriteField(dataBuffer, ref dataOffset, policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			WriteField(dataBuffer, ref dataOffset, taskId, FieldType.TRAN_ID);

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					WriteOperation(dataBuffer, ref dataOffset, binName, Operation.Type.READ);
				}
			}
			command.End(dataBuffer, ref dataOffset);
		}

		//--------------------------------------------------
		// Query
		//--------------------------------------------------

		public static void SetQuery
		(
			this CommandNew command,
			byte[] dataBuffer, 
			ref int dataOffset,
			Cluster cluster,
			Policy policy,
			Statement statement,
			ulong taskId,
			bool background,
			NodePartitions nodePartitions
		)
		{
			byte[] functionArgBuffer = null;
			int fieldCount = 0;
			int filterSize = 0;
			int binNameSize = 0;
			bool isNew = cluster.hasPartitionQuery;

			Begin(ref dataOffset);

			if (statement.ns != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.setName != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate recordsPerSecond field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			if (statement.recordsPerSecond > 0)
			{
				dataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate socket timeout field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			dataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate taskId field.
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			byte[] packedCtx = null;

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				// Estimate INDEX_TYPE field.
				if (type != IndexCollectionType.DEFAULT)
				{
					dataOffset += FIELD_HEADER_SIZE + 1;
					fieldCount++;
				}

				// Estimate INDEX_RANGE field.
				dataOffset += FIELD_HEADER_SIZE;
				filterSize++; // num filters
				filterSize += statement.filter.EstimateSize();
				dataOffset += filterSize;
				fieldCount++;

				if (!isNew)
				{
					// Query bin names are specified as a field (Scan bin names are specified later as operations)
					// in old servers. Estimate size for selected bin names.
					if (statement.binNames != null && statement.binNames.Length > 0)
					{
						dataOffset += FIELD_HEADER_SIZE;
						binNameSize++; // num bin names

						foreach (string binName in statement.binNames)
						{
							binNameSize += ByteUtil.EstimateSizeUtf8(binName) + 1;
						}
						dataOffset += binNameSize;
						fieldCount++;
					}
				}

				packedCtx = statement.filter.PackedCtx;

				if (packedCtx != null)
				{
					dataOffset += FIELD_HEADER_SIZE + packedCtx.Length;
					fieldCount++;
				}
			}

			// Estimate aggregation/background function size.
			if (statement.functionName != null)
			{
				dataOffset += FIELD_HEADER_SIZE + 1; // udf type
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.packageName) + FIELD_HEADER_SIZE;
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.functionName) + FIELD_HEADER_SIZE;

				if (statement.functionArgs.Length > 0)
				{
					functionArgBuffer = Packer.Pack(statement.functionArgs);
				}
				else
				{
					functionArgBuffer = Array.Empty<byte>();
				}
				dataOffset += FIELD_HEADER_SIZE + functionArgBuffer.Length;
				fieldCount += 4;
			}

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			long maxRecords = 0;
			int partsFullSize = 0;
			int partsPartialDigestSize = 0;
			int partsPartialBValSize = 0;

			if (nodePartitions != null)
			{
				partsFullSize = nodePartitions.partsFull.Count * 2;
				partsPartialDigestSize = nodePartitions.partsPartial.Count * 20;

				if (statement.filter != null)
				{
					partsPartialBValSize = nodePartitions.partsPartial.Count * 8;
				}
				maxRecords = nodePartitions.recordMax;
			}

			if (partsFullSize > 0)
			{
				dataOffset += partsFullSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsPartialDigestSize > 0)
			{
				dataOffset += partsPartialDigestSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsPartialBValSize > 0)
			{
				dataOffset += partsPartialBValSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate max records field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			if (maxRecords > 0)
			{
				dataOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
			int operationCount = 0;

			if (statement.operations != null)
			{
				// Estimate size for background operations.
				if (!background)
				{
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Operations not allowed in foreground query");
				}

				foreach (Operation operation in statement.operations)
				{
					if (!Operation.IsWrite(operation.type))
					{
						throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Read operations not allowed in background query");
					}
					EstimateOperationSize(ref dataOffset, operation);
				}
				operationCount = statement.operations.Length;
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				// Estimate size for selected bin names (query bin names already handled for old servers).
				foreach (string binName in statement.binNames)
				{
					EstimateOperationSize(ref dataOffset, binName);
				}
				operationCount = statement.binNames.Length;
			}

			command.SizeBuffer();

			if (background)
			{
				WriteHeaderWrite(command, dataBuffer, ref dataOffset, (WritePolicy)policy, INFO2_WRITE, fieldCount, operationCount);
			}
			else
			{
				QueryPolicy qp = (QueryPolicy)policy;
				int readAttr = INFO1_READ;
				int writeAttr = 0;

				if (!qp.includeBinData)
				{
					readAttr |= INFO1_NOBINDATA;
				}

				if (qp.shortQuery || qp.expectedDuration == QueryDuration.SHORT)
				{
					readAttr |= INFO1_SHORT_QUERY;
				}
				else if (qp.expectedDuration == QueryDuration.LONG_RELAX_AP)
				{
					writeAttr |= INFO2_RELAX_AP_LONG_QUERY;
				}

				int infoAttr = isNew ? INFO3_PARTITION_DONE : 0;

				WriteHeaderRead(dataBuffer, ref dataOffset, policy, command.TotalTimeout, readAttr, writeAttr, infoAttr, fieldCount, operationCount);
			}

			if (statement.ns != null)
			{
				WriteField(dataBuffer, ref dataOffset, statement.ns, FieldType.NAMESPACE);
			}

			if (statement.setName != null)
			{
				WriteField(dataBuffer, ref dataOffset, statement.setName, FieldType.TABLE);
			}

			// Write records per second.
			if (statement.recordsPerSecond > 0)
			{
				WriteField(dataBuffer, ref dataOffset, statement.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			// Write socket idle timeout.
			WriteField(dataBuffer, ref dataOffset, policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			WriteField(dataBuffer, ref dataOffset, taskId, FieldType.TRAN_ID);

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				if (type != IndexCollectionType.DEFAULT)
				{
					WriteFieldHeader(dataBuffer, ref dataOffset, 1, FieldType.INDEX_TYPE);
					dataBuffer[dataOffset++] = (byte)type;
				}

				WriteFieldHeader(dataBuffer, ref dataOffset, filterSize, FieldType.INDEX_RANGE);
				dataBuffer[dataOffset++] = (byte)1;
				dataOffset = statement.filter.Write(dataBuffer, dataOffset);

				if (!isNew)
				{
					// Query bin names are specified as a field (Scan bin names are specified later as operations)
					// in old servers.
					if (statement.binNames != null && statement.binNames.Length > 0)
					{
						WriteFieldHeader(dataBuffer, ref dataOffset, binNameSize, FieldType.QUERY_BINLIST);
						dataBuffer[dataOffset++] = (byte)statement.binNames.Length;

						foreach (string binName in statement.binNames)
						{
							int len = ByteUtil.StringToUtf8(binName, dataBuffer, dataOffset + 1);
							dataBuffer[dataOffset] = (byte)len;
							dataOffset += len + 1;
						}
					}
				}

				if (packedCtx != null)
				{
					WriteFieldHeader(dataBuffer, ref dataOffset, packedCtx.Length, FieldType.INDEX_CONTEXT);
					Array.Copy(packedCtx, 0, dataBuffer, dataOffset, packedCtx.Length);
					dataOffset += packedCtx.Length;
				}
			}

			if (statement.functionName != null)
			{
				WriteFieldHeader(dataBuffer, ref dataOffset, 1, FieldType.UDF_OP);
				dataBuffer[dataOffset++] = background ? (byte)2 : (byte)1;
				WriteField(dataBuffer, ref dataOffset, statement.packageName, FieldType.UDF_PACKAGE_NAME);
				WriteField(dataBuffer, ref dataOffset, statement.functionName, FieldType.UDF_FUNCTION);
				WriteField(dataBuffer, ref dataOffset, functionArgBuffer, FieldType.UDF_ARGLIST);
			}

			policy.filterExp?.Write(command);

			if (partsFullSize > 0)
			{
				WriteFieldHeader(dataBuffer, ref dataOffset, partsFullSize, FieldType.PID_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsFull)
				{
					ByteUtil.ShortToLittleBytes((ushort)part.id, dataBuffer, dataOffset);
					dataOffset += 2;
				}
			}

			if (partsPartialDigestSize > 0)
			{
				WriteFieldHeader(dataBuffer, ref dataOffset, partsPartialDigestSize, FieldType.DIGEST_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial)
				{
					Array.Copy(part.digest, 0, dataBuffer, dataOffset, 20);
					dataOffset += 20;
				}
			}

			if (partsPartialBValSize > 0)
			{
				WriteFieldHeader(dataBuffer, ref dataOffset, partsPartialBValSize, FieldType.BVAL_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial)
				{
					ByteUtil.LongToLittleBytes(part.bval, dataBuffer, dataOffset);
					dataOffset += 8;
				}
			}

			if (maxRecords > 0)
			{
				WriteField(dataBuffer, ref dataOffset, (ulong)maxRecords, FieldType.MAX_RECORDS);
			}

			if (statement.operations != null)
			{
				foreach (Operation operation in statement.operations)
				{
					WriteOperation(dataBuffer, ref dataOffset, operation);
				}
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				foreach (string binName in statement.binNames)
				{
					WriteOperation(dataBuffer, ref dataOffset, binName, Operation.Type.READ);
				}
			}
			command.End(dataBuffer, ref dataOffset);
		}

		//--------------------------------------------------
		// CommandNew Sizing
		//--------------------------------------------------

		private static int EstimateKeySize(ref int dataOffset, Policy policy, Key key)
		{
			int fieldCount = 0;

			if (key.ns != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (key.setName != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			dataOffset += key.digest.Length + FIELD_HEADER_SIZE;
			fieldCount++;

			if (policy.sendKey)
			{
				dataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}
			return fieldCount;
		}

		private static int EstimateUdfSize(ref int dataOffset, string packageName, string functionName, byte[] bytes)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(packageName) + FIELD_HEADER_SIZE;
			dataOffset += ByteUtil.EstimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;
			dataOffset += bytes.Length + FIELD_HEADER_SIZE;
			return 3;
		}

		private static void EstimateOperationSize(ref int dataOffset, Bin bin)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
			dataOffset += bin.value.EstimateSize();
		}

		private static void EstimateOperationSize(ref int dataOffset, Operation operation)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
			dataOffset += operation.value.EstimateSize();
		}

		private static void EstimateReadOperationSize(ref int dataOffset, Operation operation)
		{
			if (Operation.IsWrite(operation.type))
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
			}
			dataOffset += ByteUtil.EstimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
			dataOffset += operation.value.EstimateSize();
		}

		private static void EstimateOperationSize(ref int dataOffset, string binName)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
		}

		private static void EstimateOperationSize(ref int dataOffset)
		{
			dataOffset += OPERATION_HEADER_SIZE;
		}

		//--------------------------------------------------
		// CommandNew Writes
		//--------------------------------------------------

		/// <summary>
		/// Header write for write commands.
		/// </summary>
		private static void WriteHeaderWrite(CommandNew command, byte[] dataBuffer, ref int dataOffset, WritePolicy policy, int writeAttr, int fieldCount, int operationCount)
		{
			// Set flags.
			int generation = 0;
			int infoAttr = 0;

			switch (policy.recordExistsAction)
			{
			case RecordExistsAction.UPDATE:
				break;
			case RecordExistsAction.UPDATE_ONLY:
				infoAttr |= INFO3_UPDATE_ONLY;
				break;
			case RecordExistsAction.REPLACE:
				infoAttr |= INFO3_CREATE_OR_REPLACE;
				break;
			case RecordExistsAction.REPLACE_ONLY:
				infoAttr |= INFO3_REPLACE_ONLY;
				break;
			case RecordExistsAction.CREATE_ONLY:
				writeAttr |= INFO2_CREATE_ONLY;
				break;
			}

			switch (policy.generationPolicy)
			{
			case GenerationPolicy.NONE:
				break;
			case GenerationPolicy.EXPECT_GEN_EQUAL:
				generation = policy.generation;
				writeAttr |= INFO2_GENERATION;
				break;
			case GenerationPolicy.EXPECT_GEN_GT:
				generation = policy.generation;
				writeAttr |= INFO2_GENERATION_GT;
				break;
			}

			if (policy.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= INFO3_COMMIT_MASTER;
			}

			if (policy.durableDelete)
			{
				writeAttr |= INFO2_DURABLE_DELETE;
			}

			dataOffset += 8;

			// Write all header data except total size which must be written last. 
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)writeAttr;
			dataBuffer[dataOffset++] = (byte)infoAttr;
			dataBuffer[dataOffset++] = 0; // unused
			dataBuffer[dataOffset++] = 0; // clear the result code
			dataOffset += ByteUtil.IntToBytes((uint)generation, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)policy.expiration, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)command.ServerTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for operate command.
		/// </summary>
		private static void WriteHeaderReadWrite
		(
			CommandNew command,
			byte[] dataBuffer, 
			ref int dataOffset,
			WritePolicy policy,
			OperateArgs args,
			int fieldCount
		)
		{
			// Set flags.
			int generation = 0;
			int ttl = args.hasWrite ? policy.expiration : policy.readTouchTtlPercent;
			int readAttr = args.readAttr;
			int writeAttr = args.writeAttr;
			int infoAttr = 0;
			int operationCount = args.operations.Length;

			switch (policy.recordExistsAction)
			{
				case RecordExistsAction.UPDATE:
					break;
				case RecordExistsAction.UPDATE_ONLY:
					infoAttr |= INFO3_UPDATE_ONLY;
					break;
				case RecordExistsAction.REPLACE:
					infoAttr |= INFO3_CREATE_OR_REPLACE;
					break;
				case RecordExistsAction.REPLACE_ONLY:
					infoAttr |= INFO3_REPLACE_ONLY;
					break;
				case RecordExistsAction.CREATE_ONLY:
					writeAttr |= INFO2_CREATE_ONLY;
					break;
			}

			switch (policy.generationPolicy)
			{
				case GenerationPolicy.NONE:
					break;
				case GenerationPolicy.EXPECT_GEN_EQUAL:
					generation = policy.generation;
					writeAttr |= INFO2_GENERATION;
					break;
				case GenerationPolicy.EXPECT_GEN_GT:
					generation = policy.generation;
					writeAttr |= INFO2_GENERATION_GT;
					break;
			}

			if (policy.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= INFO3_COMMIT_MASTER;
			}

			if (policy.durableDelete)
			{
				writeAttr |= INFO2_DURABLE_DELETE;
			}

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |=	INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			if (policy.compress)
			{
				readAttr |= INFO1_COMPRESS_RESPONSE;
			}

			dataOffset += 8;

			// Write all header data except total size which must be written last. 
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)readAttr;
			dataBuffer[dataOffset++] = (byte)writeAttr;
			dataBuffer[dataOffset++] = (byte)infoAttr;
			dataBuffer[dataOffset++] = 0; // unused
			dataBuffer[dataOffset++] = 0; // clear the result code
			dataOffset += ByteUtil.IntToBytes((uint)generation, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)ttl, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)command.ServerTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for read commands.
		/// </summary>
		private static void WriteHeaderRead
		(
			byte[] dataBuffer, 
			ref int dataOffset,
			Policy policy,
			int timeout,
			int readAttr,
			int writeAttr,
			int infoAttr,
			int fieldCount,
			int operationCount
		)
		{
			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			if (policy.compress)
			{
				readAttr |= INFO1_COMPRESS_RESPONSE;
			}

			dataOffset += 8;

			// Write all header data except total size which must be written last. 
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)readAttr;
			dataBuffer[dataOffset++] = (byte)writeAttr;
			dataBuffer[dataOffset++] = (byte)infoAttr;

			for (int i = 0; i < 6; i++)
			{
				dataBuffer[dataOffset++] = 0;
			}
			dataOffset += ByteUtil.IntToBytes((uint)policy.readTouchTtlPercent, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)timeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for read header commands.
		/// </summary>
		private static void WriteHeaderReadHeader(this CommandNew command, byte[] dataBuffer, ref int dataOffset, Policy policy, int readAttr, int fieldCount, int operationCount)
		{
			int infoAttr = 0;

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			dataOffset += 8;

			// Write all header data except total size which must be written last. 
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)readAttr;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)infoAttr;

			for (int i = 0; i < 6; i++)
			{
				dataBuffer[dataOffset++] = 0;
			}
			dataOffset += ByteUtil.IntToBytes((uint)policy.readTouchTtlPercent, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)command.ServerTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		private static void WriteKey(byte[] dataBuffer, ref int dataOffset, Policy policy, Key key)
		{
			// Write key into dataBuffer.
			if (key.ns != null)
			{
				WriteField(dataBuffer, ref dataOffset, key.ns, FieldType.NAMESPACE);
			}

			if (key.setName != null)
			{
				WriteField(dataBuffer, ref dataOffset, key.setName, FieldType.TABLE);
			}

			WriteField(dataBuffer, ref dataOffset, key.digest, FieldType.DIGEST_RIPE);

			if (policy.sendKey)
			{
				WriteField(dataBuffer, ref dataOffset, key.userKey, FieldType.KEY);
			}
		}

		private static int WriteReadOnlyOperations(byte[] dataBuffer, ref int dataOffset, Operation[] ops, int readAttr)
		{
			bool readBin = false;
			bool readHeader = false;

			foreach (Operation op in ops)
			{
				switch (op.type)
				{
					case Operation.Type.READ:
						// Read all bins if no bin is specified.
						if (op.binName == null)
						{
							readAttr |= INFO1_GET_ALL;
						}
						readBin = true;
						break;

					case Operation.Type.READ_HEADER:
						readHeader = true;
						break;

					default:
						break;
				}
				WriteOperation(dataBuffer, ref dataOffset, op);
			}

			if (readHeader && !readBin)
			{
				readAttr |= INFO1_NOBINDATA;
			}
			return readAttr;
		}

		private static void WriteOperation(byte[] dataBuffer, ref int dataOffset, Bin bin, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(bin.name, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);
			int valueLength = bin.value.Write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operationType);
			dataBuffer[dataOffset++] = (byte) bin.value.Type;
			dataBuffer[dataOffset++] = (byte) 0;
			dataBuffer[dataOffset++] = (byte) nameLength;
			dataOffset += nameLength + valueLength;
		}

		private static void WriteOperation(byte[] dataBuffer, ref int dataOffset, Operation operation)
		{
			int nameLength = ByteUtil.StringToUtf8(operation.binName, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);
			int valueLength = operation.value.Write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operation.type);
			dataBuffer[dataOffset++] = (byte) operation.value.Type;
			dataBuffer[dataOffset++] = (byte) 0;
			dataBuffer[dataOffset++] = (byte) nameLength;
			dataOffset += nameLength + valueLength;
		}

		private static void WriteOperation(byte[] dataBuffer, ref int dataOffset, string name, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(name, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);

			ByteUtil.IntToBytes((uint)(nameLength + 4), dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operationType);
			dataBuffer[dataOffset++] = (byte) 0;
			dataBuffer[dataOffset++] = (byte) 0;
			dataBuffer[dataOffset++] = (byte) nameLength;
			dataOffset += nameLength;
		}

		private static void WriteOperation(byte[] dataBuffer, ref int dataOffset, Operation.Type operationType)
		{
			ByteUtil.IntToBytes(4, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operationType);
			dataBuffer[dataOffset++] = 0;
			dataBuffer[dataOffset++] = 0;
			dataBuffer[dataOffset++] = 0;
		}

		private static void WriteField(byte[] dataBuffer, ref int dataOffset, Value value, int type)
		{
			int offset = dataOffset + FIELD_HEADER_SIZE;
			dataBuffer[offset++] = (byte)value.Type;
			int len = value.Write(dataBuffer, offset) + 1;
			WriteFieldHeader(dataBuffer, ref dataOffset, len, type);
			dataOffset += len;
		}

		private static void WriteField(byte[] dataBuffer, ref int dataOffset, string str, int type)
		{
			int len = ByteUtil.StringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
			WriteFieldHeader(dataBuffer, ref dataOffset, len, type);
			dataOffset += len;
		}

		private static void WriteField(byte[] dataBuffer, ref int dataOffset, byte[] bytes, int type)
		{
			Array.Copy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.Length);
			WriteFieldHeader(dataBuffer, ref dataOffset, bytes.Length, type);
			dataOffset += bytes.Length;
		}

		private static void WriteField(byte[] dataBuffer, ref int dataOffset, int val, int type)
		{
			WriteFieldHeader(dataBuffer, ref dataOffset, 4, type);
			dataOffset += ByteUtil.IntToBytes((uint)val, dataBuffer, dataOffset);
		}

		private static void WriteField(byte[] dataBuffer, ref int dataOffset, ulong val, int type)
		{
			WriteFieldHeader(dataBuffer, ref dataOffset, 8, type);
			dataOffset += ByteUtil.LongToBytes(val, dataBuffer, dataOffset);
		}

		private static void WriteFieldHeader(byte[] dataBuffer, ref int dataOffset, int size, int type)
		{
			dataOffset += ByteUtil.IntToBytes((uint)size + 1, dataBuffer, dataOffset);
			dataBuffer[dataOffset++] = (byte)type;
		}

		internal static void WriteExpHeader(this CommandNew command, byte[] dataBuffer, ref int dataOffset, int size)
		{
			WriteFieldHeader(dataBuffer, ref dataOffset, size, FieldType.FILTER_EXP);
		}

		private static void Begin(ref int dataOffset)
		{
			dataOffset = MSG_TOTAL_HEADER_SIZE;
		}

		private static bool SizeBuffer(CommandNew command, ref byte[] dataBuffer, ref int dataOffset, Policy policy)
		{
			if (policy.compress && dataOffset > COMPRESS_THRESHOLD)
			{
				// CommandNew will be compressed. First, write uncompressed command
				// into separate dataBuffer. Save normal dataBuffer for compressed command.
				// Normal dataBuffer in async mode is from dataBuffer pool that is used to
				// minimize memory pinning during socket operations.
				dataBuffer = command.BufferPool.Rent(dataOffset);
				dataOffset = 0;
				return true;
			}
			else
			{
				// CommandNew will be uncompressed.
				command.SizeBuffer();
				return false;
			}
		}

		private static void End(CommandNew command, byte[] dataBuffer, ref int dataOffset, bool compress)
		{
			if (!compress)
			{
				command.End(dataBuffer, ref dataOffset);
				return;
			}

			// Write proto header.
			ulong size = ((ulong)dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);

			byte[] srcBuf = dataBuffer;
			int srcSize = dataOffset;

			// Increase requested dataBuffer size in case compressed dataBuffer size is
			// greater than the uncompressed dataBuffer size.
			dataOffset += 16 + 100;

			// This method finds dataBuffer of requested size, resets dataOffset to segment offset
			// and returns dataBuffer max size;
			int trgBufSize = command.SizeBuffer();

			// Compress to target starting at new dataOffset plus new header.
			int trgSize = ByteUtil.Compress(srcBuf, srcSize, dataBuffer, dataOffset + 16, trgBufSize - 16) + 16;

			ulong proto = ((ulong)trgSize - 8) | (CL_MSG_VERSION << 56) | (MSG_TYPE_COMPRESSED << 48);
			ByteUtil.LongToBytes(proto, dataBuffer, dataOffset);
			ByteUtil.LongToBytes((ulong)srcSize, dataBuffer, dataOffset + 8);
			command.SetLength(dataBuffer, ref dataOffset, trgSize);
		}

		
		//--------------------------------------------------
		// Response Parsing
		//--------------------------------------------------

		internal static void SkipKey(this CommandNew command, byte[] dataBuffer, ref int dataOffset, int fieldCount)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldlen;
			}
		}

		internal static Key ParseKey(byte[] dataBuffer, ref int dataOffset, int fieldCount, out ulong bval)
		{
			byte[] digest = null;
			string ns = null;
			string setName = null;
			Value userKey = null;
			bval = 0;

			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int fieldtype = dataBuffer[dataOffset++];
				int size = fieldlen - 1;

				switch (fieldtype)
				{
					case FieldType.DIGEST_RIPE:
						digest = new byte[size];
						Array.Copy(dataBuffer, dataOffset, digest, 0, size);
						break;

					case FieldType.NAMESPACE:
						ns = ByteUtil.Utf8ToString(dataBuffer, dataOffset, size);
						break;

					case FieldType.TABLE:
						setName = ByteUtil.Utf8ToString(dataBuffer, dataOffset, size);
						break;

					case FieldType.KEY:
						int type = dataBuffer[dataOffset++];
						size--;
						userKey = ByteUtil.BytesToKeyValue((ParticleType)type, dataBuffer, dataOffset, size);
						break;

					case FieldType.BVAL_ARRAY:
						bval = (ulong)ByteUtil.LittleBytesToLong(dataBuffer, dataOffset);
						break;
				}
				dataOffset += size;
			}
			return new Key(ns, digest, setName, userKey);
		}
	}
}
