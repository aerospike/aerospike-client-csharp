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
using System;
using System.Collections.Generic;
using System.Text;

namespace Aerospike.Client
{
	public abstract class Command
	{
		public static readonly int INFO1_READ              = (1 << 0); // Contains a read operation.
		public static readonly int INFO1_GET_ALL           = (1 << 1); // Get all bins.
		public static readonly int INFO1_BATCH             = (1 << 3); // Batch read or exists.
		public static readonly int INFO1_NOBINDATA         = (1 << 5); // Do not read the bins.
		public static readonly int INFO1_READ_MODE_AP_ALL  = (1 << 6); // Involve all replicas in read operation.
		public static readonly int INFO1_COMPRESS_RESPONSE = (1 << 7); // Tell server to compress it's response.

		public static readonly int INFO2_WRITE             = (1 << 0); // Create or update record
		public static readonly int INFO2_DELETE            = (1 << 1); // Fling a record into the belly of Moloch.
		public static readonly int INFO2_GENERATION        = (1 << 2); // Update if expected generation == old.
		public static readonly int INFO2_GENERATION_GT     = (1 << 3); // Update if new generation >= old, good for restore.
		public static readonly int INFO2_DURABLE_DELETE    = (1 << 4); // Transaction resulting in record deletion leaves tombstone (Enterprise only).
		public static readonly int INFO2_CREATE_ONLY       = (1 << 5); // Create only. Fail if record already exists.
		public static readonly int INFO2_RESPOND_ALL_OPS   = (1 << 7); // Return a result for every operation.

		public static readonly int INFO3_LAST              = (1 << 0); // This is the last of a multi-part message.
		public static readonly int INFO3_COMMIT_MASTER     = (1 << 1); // Commit to master only before declaring success.
		public static readonly int INFO3_PARTITION_DONE    = (1 << 2); // Partition is complete response in scan.
		public static readonly int INFO3_UPDATE_ONLY       = (1 << 3); // Update only. Merge bins.
		public static readonly int INFO3_CREATE_OR_REPLACE = (1 << 4); // Create or completely replace record.
		public static readonly int INFO3_REPLACE_ONLY      = (1 << 5); // Completely replace existing record only.
		public static readonly int INFO3_SC_READ_TYPE      = (1 << 6); // See below.
		public static readonly int INFO3_SC_READ_RELAX     = (1 << 7); // See below.

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

		public const int MSG_TOTAL_HEADER_SIZE = 30;
		public const int FIELD_HEADER_SIZE = 5;
		public const int OPERATION_HEADER_SIZE = 8;
		public const int MSG_REMAINING_HEADER_SIZE = 22;
		public const int DIGEST_SIZE = 20;
		public const int COMPRESS_THRESHOLD = 128;
		public const ulong CL_MSG_VERSION = 2UL;
		public const ulong AS_MSG_TYPE = 3UL;
		public const ulong MSG_TYPE_COMPRESSED = 4UL;

		internal byte[] dataBuffer;
		internal int dataOffset;
		internal readonly int maxRetries;
		internal readonly int serverTimeout;
		internal int socketTimeout;
		internal int totalTimeout;

		public Command(int socketTimeout, int totalTimeout, int maxRetries)
		{
			this.maxRetries = maxRetries;
			this.totalTimeout = totalTimeout;

			if (totalTimeout > 0)
			{
				this.socketTimeout = (socketTimeout < totalTimeout && socketTimeout > 0) ? socketTimeout : totalTimeout;
				this.serverTimeout = this.socketTimeout;
			}
			else
			{
				this.socketTimeout = socketTimeout;
				this.serverTimeout = 0;
			}
		}

		public void SetWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}

			foreach (Bin bin in bins)
			{
				EstimateOperationSize(bin);
			}
			
			bool compress = SizeBuffer(policy);

			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, bins.Length);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}

			foreach (Bin bin in bins)
			{
				WriteOperation(bin, operation);
			}
			End(compress);
		}

		public void SetDelete(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderWrite(policy, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}
			End();
		}

		public void SetTouch(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			EstimateOperationSize();
			SizeBuffer();
			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 1);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}
			WriteOperation(Operation.Type.TOUCH);
			End();
		}

		public void SetExists(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}
			End();
		}

		public void SetRead(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderRead(policy, serverTimeout, Command.INFO1_READ | Command.INFO1_GET_ALL, fieldCount, 0);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}
			End();
		}

		public void SetRead(Policy policy, Key key, string[] binNames)
		{
			if (binNames != null)
			{
				Begin();
				int fieldCount = EstimateKeySize(policy, key);
				int predSize = 0;

				if (policy.predExp != null)
				{
					predSize = EstimatePredExp(policy.predExp);
					fieldCount++;
				}

				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
				SizeBuffer();
				WriteHeaderRead(policy, serverTimeout, Command.INFO1_READ, fieldCount, binNames.Length);
				WriteKey(policy, key);

				if (policy.predExp != null)
				{
					WritePredExp(policy.predExp, predSize);
				}

				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
				End();
			}
			else
			{
				SetRead(policy, key);
			}
		}

		public void SetReadHeader(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			EstimateOperationSize((string)null);
			SizeBuffer();
			WriteHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}
			End();
		}

		public void SetOperate(WritePolicy policy, Key key, OperateArgs args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			dataOffset += args.size;

			bool compress = SizeBuffer(policy);

			WriteHeaderReadWrite(policy, args.readAttr, args.writeAttr, fieldCount, args.operations.Length);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}

			foreach (Operation operation in args.operations)
			{
				WriteOperation(operation);
			}
			End(compress);
		}

		public void SetUdf(WritePolicy policy, Key key, string packageName, string functionName, Value[] args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			byte[] argBytes = Packer.Pack(args);
			fieldCount += EstimateUdfSize(packageName, functionName, argBytes);

			bool compress = SizeBuffer(policy);

			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 0);
			WriteKey(policy, key);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}
			WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
			WriteField(functionName, FieldType.UDF_FUNCTION);
			WriteField(argBytes, FieldType.UDF_ARGLIST);
			End(compress);
		}

		public void SetBatchRead(BatchPolicy policy, List<BatchRead> records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			ushort fieldCountRow = policy.sendSetName ? (ushort)2 : (ushort)1;
			BatchRead prev = null;

			Begin();
			int fieldCount = 1;
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			for (int i = 0; i < max; i++)
			{
				BatchRead record = records[offsets[i]];
				Key key = record.key;
				string[] binNames = record.binNames;

				dataOffset += key.digest.Length + 4;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.
				if (prev != null && prev.key.ns == key.ns &&
					(! policy.sendSetName || prev.key.setName == key.setName) &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;

					if (policy.sendSetName)
					{
						dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					}

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							EstimateOperationSize(binName);
						}
					}
					prev = record;
				}
			}

			bool compress = SizeBuffer(policy);

			int readAttr = Command.INFO1_READ;

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			WriteHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, fieldCount, 0);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, policy.sendSetName ? FieldType.BATCH_INDEX_WITH_SET : FieldType.BATCH_INDEX); // Need to update size at end

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
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.		
				if (prev != null && prev.key.ns == key.ns &&
					(!policy.sendSetName || prev.key.setName == key.setName) &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins)
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = 1; // repeat
				}
				else
				{
					// Write full header, namespace and bin names.
					dataBuffer[dataOffset++] = 0; // do not repeat

					if (binNames != null && binNames.Length != 0)
					{
						dataBuffer[dataOffset++] = (byte)readAttr;
						dataOffset += ByteUtil.ShortToBytes(fieldCountRow, dataBuffer, dataOffset);
						dataOffset += ByteUtil.ShortToBytes((ushort)binNames.Length, dataBuffer, dataOffset);
						WriteField(key.ns, FieldType.NAMESPACE);

						if (policy.sendSetName)
						{
							WriteField(key.setName, FieldType.TABLE);
						}

						foreach (string binName in binNames)
						{
							WriteOperation(binName, Operation.Type.READ);
						}
					}
					else
					{
						dataBuffer[dataOffset++] = (byte)(readAttr | (record.readAllBins ? Command.INFO1_GET_ALL : Command.INFO1_NOBINDATA));
						dataOffset += ByteUtil.ShortToBytes(fieldCountRow, dataBuffer, dataOffset);
						dataOffset += ByteUtil.ShortToBytes(0, dataBuffer, dataOffset);
						WriteField(key.ns, FieldType.NAMESPACE);

						if (policy.sendSetName)
						{
							WriteField(key.setName, FieldType.TABLE);
						}
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(compress);
		}

		public void SetBatchRead(BatchPolicy policy, Key[] keys, BatchNode batch, string[] binNames, int readAttr)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			ushort fieldCountRow = policy.sendSetName ? (ushort)2 : (ushort)1;

			// Calculate size of bin names.
			int binNameSize = 0;
			int operationCount = 0;
		
			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					binNameSize += ByteUtil.EstimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
				}
				operationCount = binNames.Length;
			}

			// Estimate buffer size.
			Begin();
			int fieldCount = 1;
			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}
			dataOffset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				dataOffset += key.digest.Length + 4;

				// Try reference equality in hope that namespace for all keys is set from a fixed variable.
				if (prev != null && prev.ns == key.ns && (! policy.sendSetName || prev.setName == key.setName)) 
				{	
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;

					if (policy.sendSetName)
					{
						dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					}
					dataOffset += binNameSize;
					prev = key;
				}
			}

			bool compress = SizeBuffer(policy);

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			WriteHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, fieldCount, 0);

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, policy.sendSetName ? FieldType.BATCH_INDEX_WITH_SET : FieldType.BATCH_INDEX); // Need to update size at end

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
				if (prev != null && prev.ns == key.ns && (!policy.sendSetName || prev.setName == key.setName))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = 1; // repeat
				}
				else
				{
					// Write full header, namespace and bin names.
					dataBuffer[dataOffset++] = 0; // do not repeat
					dataBuffer[dataOffset++] = (byte)readAttr;
					dataOffset += ByteUtil.ShortToBytes(fieldCountRow, dataBuffer, dataOffset);
					dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
					WriteField(key.ns, FieldType.NAMESPACE);

					if (policy.sendSetName)
					{
						WriteField(key.setName, FieldType.TABLE);
					}

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							WriteOperation(binName, Operation.Type.READ);
						}
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(compress);
		}

		public void SetScan
		(
			ScanPolicy policy,
			string ns,
			string setName,
			string[] binNames,
			ulong taskId,
			NodePartitions nodePartitions
		)
		{
			Begin();
			int fieldCount = 0;
			int partsFullSize = 0;
			int partsPartialSize = 0;
			long maxRecords = 0;

			if (nodePartitions != null)
			{
				partsFullSize = nodePartitions.partsFull.Count * 2;
				partsPartialSize = nodePartitions.partsPartial.Count * 20;
				maxRecords = nodePartitions.recordMax;
			}

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

			int predSize = 0;

			if (policy.predExp != null)
			{
				predSize = EstimatePredExp(policy.predExp);
				fieldCount++;
			}

			// Only set scan options for server versions < 4.9 or if scanPercent was modified.
			if (nodePartitions == null || policy.scanPercent < 100)
			{
				// Estimate scan options size.
				dataOffset += 2 + FIELD_HEADER_SIZE;
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
					EstimateOperationSize(binName);
				}
			}

			SizeBuffer();
			byte readAttr = (byte)Command.INFO1_READ;

			if (!policy.includeBinData)
			{
				readAttr |= (byte)Command.INFO1_NOBINDATA;
			}

			int operationCount = (binNames == null) ? 0 : binNames.Length;
			WriteHeaderRead(policy, totalTimeout, readAttr, fieldCount, operationCount);

			if (ns != null)
			{
				WriteField(ns, FieldType.NAMESPACE);
			}

			if (setName != null)
			{
				WriteField(setName, FieldType.TABLE);
			}

			if (partsFullSize > 0)
			{
				WriteFieldHeader(partsFullSize, FieldType.PID_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsFull)
				{
					ByteUtil.ShortToLittleBytes((ushort)part.id, dataBuffer, dataOffset);
					dataOffset += 2;
				}
			}

			if (partsPartialSize > 0)
			{
				WriteFieldHeader(partsPartialSize, FieldType.DIGEST_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial) {
					Array.Copy(part.digest, 0, dataBuffer, dataOffset, 20); 
					dataOffset += 20;
				}
			}

			if (maxRecords > 0)
			{
				WriteField((ulong)maxRecords, FieldType.SCAN_MAX_RECORDS);
			}

			if (policy.recordsPerSecond > 0)
			{
				WriteField(policy.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			if (policy.predExp != null)
			{
				WritePredExp(policy.predExp, predSize);
			}

			// Only set scan options for server versions < 4.9 or if scanPercent was modified.
			if (nodePartitions == null || policy.scanPercent < 100)
			{
				WriteFieldHeader(2, FieldType.SCAN_OPTIONS);

				byte priority = (byte)policy.priority;
				priority <<= 4;

				if (policy.failOnClusterChange)
				{
					priority |= 0x08;
				}
				dataBuffer[dataOffset++] = priority;
				dataBuffer[dataOffset++] = (byte)policy.scanPercent;
			}

			// Write scan timeout
			WriteField(policy.socketTimeout, FieldType.SCAN_TIMEOUT);

			// Write taskId field
			WriteField(taskId, FieldType.TRAN_ID);

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		protected internal void SetQuery(Policy policy, Statement statement, bool write, NodePartitions nodePartitions)
		{
			byte[] functionArgBuffer = null;
			int fieldCount = 0;
			int filterSize = 0;
			int binNameSize = 0;
			int partsFullSize = 0;
			int partsPartialSize = 0;
			long maxRecords = 0;

			Begin();

			if (statement.ns != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.indexName != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.indexName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.setName != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Allocate space for TaskId field.
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				if (type != IndexCollectionType.DEFAULT)
				{
					dataOffset += FIELD_HEADER_SIZE + 1;
					fieldCount++;
				}

				dataOffset += FIELD_HEADER_SIZE;
				filterSize++; // num filters
				filterSize += statement.filter.EstimateSize();
				dataOffset += filterSize;
				fieldCount++;

				// Query bin names are specified as a field (Scan bin names are specified later as operations)
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
			else
			{
				// Calling query with no filters is more efficiently handled by a primary index scan. 
				if (nodePartitions != null)
				{
					partsFullSize = nodePartitions.partsFull.Count * 2;
					partsPartialSize = nodePartitions.partsPartial.Count * 20;
					maxRecords = nodePartitions.recordMax;
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

				// Estimate max records size;
				if (maxRecords > 0)
				{
					dataOffset += 8 + FIELD_HEADER_SIZE;
					fieldCount++;
				}

				// Only set scan options for server versions < 4.9.
				if (nodePartitions == null)
				{
					// Estimate scan options size.
					dataOffset += 2 + FIELD_HEADER_SIZE;
					fieldCount++;
				}

				// Estimate scan timeout size.
				dataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;

				// Estimate records per second size.
				if (statement.recordsPerSecond > 0)
				{
					dataOffset += 4 + FIELD_HEADER_SIZE;
					fieldCount++;
				}
			}

			PredExp[] predExp = statement.PredExp;
			int predSize = 0;

			if (policy.predExp != null && predExp == null)
			{
				predExp = policy.predExp;
			}

			if (predExp != null)
			{
				predSize = EstimatePredExp(predExp);
				fieldCount++;
			}

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
					functionArgBuffer = new byte[0];
				}
				dataOffset += FIELD_HEADER_SIZE + functionArgBuffer.Length;
				fieldCount += 4;
			}

			// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
			int operationCount = 0;

			if (statement.operations != null)
			{
				foreach (Operation operation in statement.operations)
				{
					EstimateOperationSize(operation);
				}
				operationCount = statement.operations.Length;
			}
			else if (statement.binNames != null && statement.filter == null)
			{
				foreach (string binName in statement.binNames)
				{
					EstimateOperationSize(binName);
				}
				operationCount = statement.binNames.Length;
			}

			SizeBuffer();

			if (write)
			{
				WriteHeaderWrite((WritePolicy)policy, Command.INFO2_WRITE, fieldCount, operationCount);
			}
			else
			{
				QueryPolicy qp = (QueryPolicy)policy;
				int readAttr = qp.includeBinData ? Command.INFO1_READ : Command.INFO1_READ | Command.INFO1_NOBINDATA;
				WriteHeaderRead(policy, totalTimeout, readAttr, fieldCount, operationCount);
			}

			if (statement.ns != null)
			{
				WriteField(statement.ns, FieldType.NAMESPACE);
			}

			if (statement.indexName != null)
			{
				WriteField(statement.indexName, FieldType.INDEX_NAME);
			}

			if (statement.setName != null)
			{
				WriteField(statement.setName, FieldType.TABLE);
			}

			// Write taskId field
			WriteField(statement.taskId, FieldType.TRAN_ID);

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				if (type != IndexCollectionType.DEFAULT)
				{
					WriteFieldHeader(1, FieldType.INDEX_TYPE);
					dataBuffer[dataOffset++] = (byte)type;
				}

				WriteFieldHeader(filterSize, FieldType.INDEX_RANGE);
				dataBuffer[dataOffset++] = (byte)1;
				dataOffset = statement.filter.Write(dataBuffer, dataOffset);

				// Query bin names are specified as a field (Scan bin names are specified later as operations)
				if (statement.binNames != null && statement.binNames.Length > 0)
				{
					WriteFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
					dataBuffer[dataOffset++] = (byte)statement.binNames.Length;

					foreach (string binName in statement.binNames)
					{
						int len = ByteUtil.StringToUtf8(binName, dataBuffer, dataOffset + 1);
						dataBuffer[dataOffset] = (byte)len;
						dataOffset += len + 1;
					}
				}
			}
			else
			{
				// Calling query with no filters is more efficiently handled by a primary index scan. 
				if (partsFullSize > 0)
				{
					WriteFieldHeader(partsFullSize, FieldType.PID_ARRAY);

					foreach (PartitionStatus part in nodePartitions.partsFull)
					{
						ByteUtil.ShortToLittleBytes((ushort)part.id, dataBuffer, dataOffset);
						dataOffset += 2;
					}
				}

				if (partsPartialSize > 0)
				{
					WriteFieldHeader(partsPartialSize, FieldType.DIGEST_ARRAY);

					foreach (PartitionStatus part in nodePartitions.partsPartial)
					{
						Array.Copy(part.digest, 0, dataBuffer, dataOffset, 20);
						dataOffset += 20;
					}
				}

				if (maxRecords > 0)
				{
					WriteField((ulong)maxRecords, FieldType.SCAN_MAX_RECORDS);
				}

				// Only set scan options for server versions < 4.9.
				if (nodePartitions == null)
				{
					WriteFieldHeader(2, FieldType.SCAN_OPTIONS);
					byte priority = (byte)policy.priority;
					priority <<= 4;

					if (!write && ((QueryPolicy)policy).failOnClusterChange)
					{
						priority |= 0x08;
					}
					dataBuffer[dataOffset++] = priority;
					dataBuffer[dataOffset++] = (byte)100;
				}

				// Write scan socket idle timeout.
				WriteField(policy.socketTimeout, FieldType.SCAN_TIMEOUT);

				// Write records per second.
				if (statement.recordsPerSecond > 0)
				{
					WriteField(statement.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
				}
			}

			if (predExp != null)
			{
				WritePredExp(predExp, predSize);
			}

			if (statement.functionName != null)
			{
				WriteFieldHeader(1, FieldType.UDF_OP);
				dataBuffer[dataOffset++] = (statement.returnData) ? (byte)1 : (byte)2;
				WriteField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
				WriteField(statement.functionName, FieldType.UDF_FUNCTION);
				WriteField(functionArgBuffer, FieldType.UDF_ARGLIST);
			}

			if (statement.operations != null)
			{
				foreach (Operation operation in statement.operations)
				{
					WriteOperation(operation);
				}
			}
			else if (statement.binNames != null && statement.filter == null)
			{
				// Scan bin names are specified after all fields.
				foreach (string binName in statement.binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		private int EstimateKeySize(Policy policy, Key key)
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

		private int EstimateUdfSize(string packageName, string functionName, byte[] bytes)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(packageName) + FIELD_HEADER_SIZE;
			dataOffset += ByteUtil.EstimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;
			dataOffset += bytes.Length + FIELD_HEADER_SIZE;
			return 3;
		}

		private int EstimatePredExp(PredExp[] predExp)
		{
			int sz = PredExp.EstimateSize(predExp);
			dataOffset += sz + FIELD_HEADER_SIZE;
			return sz;
		}

		private void EstimateOperationSize(Bin bin)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
			dataOffset += bin.value.EstimateSize();
		}

		private void EstimateOperationSize(Operation operation)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
			dataOffset += operation.value.EstimateSize();
		}

		private void EstimateOperationSize(string binName)
		{
			dataOffset += ByteUtil.EstimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
		}

		private void EstimateOperationSize()
		{
			dataOffset += OPERATION_HEADER_SIZE;
		}

		/// <summary>
		/// Header write for write commands.
		/// </summary>
		private void WriteHeaderWrite(WritePolicy policy, int writeAttr, int fieldCount, int operationCount)
		{
			// Set flags.
			int generation = 0;
			int infoAttr = 0;

			switch (policy.recordExistsAction)
			{
			case RecordExistsAction.UPDATE:
				break;
			case RecordExistsAction.UPDATE_ONLY:
				infoAttr |= Command.INFO3_UPDATE_ONLY;
				break;
			case RecordExistsAction.REPLACE:
				infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
				break;
			case RecordExistsAction.REPLACE_ONLY:
				infoAttr |= Command.INFO3_REPLACE_ONLY;
				break;
			case RecordExistsAction.CREATE_ONLY:
				writeAttr |= Command.INFO2_CREATE_ONLY;
				break;
			}

			switch (policy.generationPolicy)
			{
			case GenerationPolicy.NONE:
				break;
			case GenerationPolicy.EXPECT_GEN_EQUAL:
				generation = policy.generation;
				writeAttr |= Command.INFO2_GENERATION;
				break;
			case GenerationPolicy.EXPECT_GEN_GT:
				generation = policy.generation;
				writeAttr |= Command.INFO2_GENERATION_GT;
				break;
			}

			if (policy.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= Command.INFO3_COMMIT_MASTER;
			}

			if (policy.durableDelete)
			{
				writeAttr |= Command.INFO2_DURABLE_DELETE;
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
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for operate command.
		/// </summary>
		private void WriteHeaderReadWrite(WritePolicy policy, int readAttr, int writeAttr, int fieldCount, int operationCount)
		{
			// Set flags.
			int generation = 0;
			int infoAttr = 0;

			switch (policy.recordExistsAction)
			{
				case RecordExistsAction.UPDATE:
					break;
				case RecordExistsAction.UPDATE_ONLY:
					infoAttr |= Command.INFO3_UPDATE_ONLY;
					break;
				case RecordExistsAction.REPLACE:
					infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
					break;
				case RecordExistsAction.REPLACE_ONLY:
					infoAttr |= Command.INFO3_REPLACE_ONLY;
					break;
				case RecordExistsAction.CREATE_ONLY:
					writeAttr |= Command.INFO2_CREATE_ONLY;
					break;
			}

			switch (policy.generationPolicy)
			{
				case GenerationPolicy.NONE:
					break;
				case GenerationPolicy.EXPECT_GEN_EQUAL:
					generation = policy.generation;
					writeAttr |= Command.INFO2_GENERATION;
					break;
				case GenerationPolicy.EXPECT_GEN_GT:
					generation = policy.generation;
					writeAttr |= Command.INFO2_GENERATION_GT;
					break;
			}

			if (policy.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= Command.INFO3_COMMIT_MASTER;
			}

			if (policy.durableDelete)
			{
				writeAttr |= Command.INFO2_DURABLE_DELETE;
			}

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= Command.INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= Command.INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			if (policy.compress)
			{
				readAttr |= Command.INFO1_COMPRESS_RESPONSE;
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
			dataOffset += ByteUtil.IntToBytes((uint)policy.expiration, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for read commands.
		/// </summary>
		private void WriteHeaderRead(Policy policy, int timeout, int readAttr, int fieldCount, int operationCount)
		{
			int infoAttr = 0;

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= Command.INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= Command.INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			if (policy.compress)
			{
				readAttr |= Command.INFO1_COMPRESS_RESPONSE;
			}

			dataOffset += 8;

			// Write all header data except total size which must be written last. 
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)readAttr;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)infoAttr;

			for (int i = 0; i < 10; i++)
			{
				dataBuffer[dataOffset++] = 0;
			}
			dataOffset += ByteUtil.IntToBytes((uint)timeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for read header commands.
		/// </summary>
		private void WriteHeaderReadHeader(Policy policy, int readAttr, int fieldCount, int operationCount)
		{
			int infoAttr = 0;

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= Command.INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= Command.INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			dataOffset += 8;

			// Write all header data except total size which must be written last. 
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)readAttr;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)infoAttr;

			for (int i = 0; i < 10; i++)
			{
				dataBuffer[dataOffset++] = 0;
			}
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		private void WriteKey(Policy policy, Key key)
		{
			// Write key into buffer.
			if (key.ns != null)
			{
				WriteField(key.ns, FieldType.NAMESPACE);
			}

			if (key.setName != null)
			{
				WriteField(key.setName, FieldType.TABLE);
			}

			WriteField(key.digest, FieldType.DIGEST_RIPE);

			if (policy.sendKey)
			{
				WriteField(key.userKey, FieldType.KEY);
			}
		}

		private void WritePredExp(PredExp[] predExp, int predSize)
		{
			WriteFieldHeader(predSize, FieldType.PREDEXP);
			dataOffset = PredExp.Write(predExp, dataBuffer, dataOffset);
		}
	
		private void WriteOperation(Bin bin, Operation.Type operationType)
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

		private void WriteOperation(Operation operation)
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

		private void WriteOperation(string name, Operation.Type operationType)
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

		private void WriteOperation(Operation.Type operationType)
		{
			ByteUtil.IntToBytes(4, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operationType);
			dataBuffer[dataOffset++] = 0;
			dataBuffer[dataOffset++] = 0;
			dataBuffer[dataOffset++] = 0;
		}

		private void WriteField(Value value, int type)
		{
			int offset = dataOffset + FIELD_HEADER_SIZE;
			dataBuffer[offset++] = (byte)value.Type;
			int len = value.Write(dataBuffer, offset) + 1;
			WriteFieldHeader(len, type);
			dataOffset += len;
		}

		private void WriteField(string str, int type)
		{
			int len = ByteUtil.StringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
			WriteFieldHeader(len, type);
			dataOffset += len;
		}

		private void WriteField(byte[] bytes, int type)
		{
			Array.Copy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.Length);
			WriteFieldHeader(bytes.Length, type);
			dataOffset += bytes.Length;
		}

		private void WriteField(int val, int type)
		{
			WriteFieldHeader(4, type);
			dataOffset += ByteUtil.IntToBytes((uint)val, dataBuffer, dataOffset);
		}

		private void WriteField(ulong val, int type)
		{
			WriteFieldHeader(8, type);
			dataOffset += ByteUtil.LongToBytes(val, dataBuffer, dataOffset);
		}

		private void WriteFieldHeader(int size, int type)
		{
			dataOffset += ByteUtil.IntToBytes((uint)size + 1, dataBuffer, dataOffset);
			dataBuffer[dataOffset++] = (byte)type;
		}

		private void Begin()
		{
			dataOffset = MSG_TOTAL_HEADER_SIZE;
		}

		internal Key ParseKey(int fieldCount)
		{
			byte[] digest = null;
			string ns = null;
			string setName = null;
			Value userKey = null;

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
						userKey = ByteUtil.BytesToKeyValue(type, dataBuffer, dataOffset, size);
						break;
				}
				dataOffset += size;
			}
			return new Key(ns, digest, setName, userKey);
		}

		private bool SizeBuffer(Policy policy)
		{
			if (policy.compress && dataOffset > COMPRESS_THRESHOLD)
			{
				// Command will be compressed. First, write uncompressed command
				// into separate buffer. Save normal buffer for compressed command.
				// Normal buffer in async mode is from buffer pool that is used to
				// minimize memory pinning during socket operations.
				dataBuffer = new byte[dataOffset];
				dataOffset = 0;
				return true;
			}
			else
			{
				// Command will be uncompressed.
				SizeBuffer();
				return false;
			}
		}

		private void End(bool compress)
		{
			if (!compress)
			{
				End();
				return;
			}

			// Write proto header.
			ulong size = ((ulong)dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);

			byte[] srcBuf = dataBuffer;
			int srcSize = dataOffset;

			// Increase requested buffer size in case compressed buffer size is
			// greater than the uncompressed buffer size.
			dataOffset += 16 + 100;

			// This method finds dataBuffer of requested size, resets dataOffset to segment offset
			// and returns dataBuffer max size;
			int trgBufSize = SizeBuffer();

			// Compress to target starting at new dataOffset plus new header.
			int trgSize = ByteUtil.Compress(srcBuf, srcSize, dataBuffer, dataOffset + 16, trgBufSize - 16) + 16;

			ulong proto = ((ulong)trgSize - 8) | (CL_MSG_VERSION << 56) | (MSG_TYPE_COMPRESSED << 48);
			ByteUtil.LongToBytes(proto, dataBuffer, dataOffset);
			ByteUtil.LongToBytes((ulong)srcSize, dataBuffer, dataOffset + 8);
			SetLength(trgSize);
		}

		protected internal abstract int SizeBuffer();
		protected internal abstract void End();
		protected internal abstract void SetLength(int length);
	}
}
