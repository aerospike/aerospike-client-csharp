/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
		// Flags commented out are not supported by this client.
		public static readonly int INFO1_READ             = (1 << 0); // Contains a read operation.
		public static readonly int INFO1_GET_ALL          = (1 << 1); // Get all bins.
		public static readonly int INFO1_BATCH            = (1 << 3); // Batch read or exists.
		public static readonly int INFO1_NOBINDATA        = (1 << 5); // Do not read the bins.
		public static readonly int INFO1_READ_MODE_AP_ALL = (1 << 6); // Involve all replicas in read operation.

		public static readonly int INFO2_WRITE           = (1 << 0); // Create or update record
		public static readonly int INFO2_DELETE          = (1 << 1); // Fling a record into the belly of Moloch.
		public static readonly int INFO2_GENERATION      = (1 << 2); // Update if expected generation == old.
		public static readonly int INFO2_GENERATION_GT   = (1 << 3); // Update if new generation >= old, good for restore.
		public static readonly int INFO2_DURABLE_DELETE  = (1 << 4); // Transaction resulting in record deletion leaves tombstone (Enterprise only).
		public static readonly int INFO2_CREATE_ONLY     = (1 << 5); // Create only. Fail if record already exists.
		public static readonly int INFO2_RESPOND_ALL_OPS = (1 << 7); // Return a result for every operation.

		public static readonly int INFO3_LAST              = (1 << 0); // This is the last of a multi-part message.
		public static readonly int INFO3_COMMIT_MASTER     = (1 << 1); // Commit to master only before declaring success.
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
		public const ulong CL_MSG_VERSION = 2L;
		public const ulong AS_MSG_TYPE = 3L;

		protected internal byte[] dataBuffer;
		protected internal int dataOffset;

		public void SetWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			
			foreach (Bin bin in bins)
			{
				EstimateOperationSize(bin);
			}
			SizeBuffer();
			WriteHeader(policy, 0, Command.INFO2_WRITE, fieldCount, bins.Length);
			WriteKey(policy, key);

			foreach (Bin bin in bins)
			{
				WriteOperation(bin, operation);
			}
			End();
		}

		public void SetDelete(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);			
			SizeBuffer();
			WriteHeader(policy, 0, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
			WriteKey(policy, key);
			End();
		}

		public void SetTouch(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			EstimateOperationSize();
			SizeBuffer();
			WriteHeader(policy, 0, Command.INFO2_WRITE, fieldCount, 1);
			WriteKey(policy, key);
			WriteOperation(Operation.Type.TOUCH);
			End();
		}

		public void SetExists(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			SizeBuffer();
			WriteHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, fieldCount, 0);
			WriteKey(policy, key);
			End();
		}

		public void SetRead(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			SizeBuffer();
			WriteHeader(policy, Command.INFO1_READ | Command.INFO1_GET_ALL, 0, fieldCount, 0);
			WriteKey(policy, key);
			End();
		}

		public void SetRead(Policy policy, Key key, string[] binNames)
		{
			if (binNames != null)
			{
				Begin();
				int fieldCount = EstimateKeySize(policy, key);

				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
				SizeBuffer();
				WriteHeader(policy, Command.INFO1_READ, 0, fieldCount, binNames.Length);
				WriteKey(policy, key);

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
			EstimateOperationSize((string)null);
			SizeBuffer();
			WriteHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, fieldCount, 0);
			WriteKey(policy, key);
			End();
		}

		public void EstimateOperate(Operation[] operations, OperateArgs args)
		{
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
						args.readAttr |= Command.INFO1_READ;

						// Read all bins if no bin is specified.
						if (operation.binName == null)
						{
							args.readAttr |= Command.INFO1_GET_ALL;
						}
						readBin = true;
						break;

					case Operation.Type.CDT_READ:
					case Operation.Type.READ:
						args.readAttr |= Command.INFO1_READ;

						// Read all bins if no bin is specified.
						if (operation.binName == null)
						{
							args.readAttr |= Command.INFO1_GET_ALL;
						}
						readBin = true;
						break;

					case Operation.Type.READ_HEADER:
						args.readAttr |= Command.INFO1_READ;
						readHeader = true;
						break;

					case Operation.Type.BIT_MODIFY:
					case Operation.Type.MAP_MODIFY:
						// Map operations require respondAllOps to be true.
						respondAllOps = true;
						args.writeAttr = Command.INFO2_WRITE;
						break;

					default:
						args.writeAttr = Command.INFO2_WRITE;
						args.hasWrite = true;
						break;
				}
				EstimateOperationSize(operation);
			}
			args.size = dataOffset;

			if (readHeader && !readBin)
			{
				args.readAttr |= Command.INFO1_NOBINDATA;
			}

			if (respondAllOps)
			{
				args.writeAttr |= Command.INFO2_RESPOND_ALL_OPS;
			}
		}

		public void SetOperate(WritePolicy policy, Key key, Operation[] operations, OperateArgs args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			dataOffset += args.size;
			SizeBuffer();

			WriteHeader(policy, args.readAttr, args.writeAttr, fieldCount, operations.Length);
			WriteKey(policy, key);

			foreach (Operation operation in operations)
			{
				WriteOperation(operation);
			}
			End();
		}

		public void SetUdf(WritePolicy policy, Key key, string packageName, string functionName, Value[] args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			byte[] argBytes = Packer.Pack(args);
			fieldCount += EstimateUdfSize(packageName, functionName, argBytes);

			SizeBuffer();
			WriteHeader(policy, 0, Command.INFO2_WRITE, fieldCount, 0);
			WriteKey(policy, key);
			WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
			WriteField(functionName, FieldType.UDF_FUNCTION);
			WriteField(argBytes, FieldType.UDF_ARGLIST);
			End();
		}

		public void SetBatchRead(BatchPolicy policy, List<BatchRead> records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			ushort fieldCount = policy.sendSetName ? (ushort)2 : (ushort)1;
			BatchRead prev = null;

			Begin();
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
			SizeBuffer();

			int readAttr = Command.INFO1_READ;

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			WriteHeader(policy, readAttr | Command.INFO1_BATCH, 0, 1, 0);
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
						dataOffset += ByteUtil.ShortToBytes(fieldCount, dataBuffer, dataOffset);
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
						dataOffset += ByteUtil.ShortToBytes(fieldCount, dataBuffer, dataOffset);
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
			End();
		}

		public void SetBatchRead(BatchPolicy policy, Key[] keys, BatchNode batch, string[] binNames, int readAttr)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			ushort fieldCount = policy.sendSetName ? (ushort)2 : (ushort)1;

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

			SizeBuffer();

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			WriteHeader(policy, readAttr | Command.INFO1_BATCH, 0, 1, 0);
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
					dataOffset += ByteUtil.ShortToBytes(fieldCount, dataBuffer, dataOffset);
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
			End();
		}

		public void SetScan(ScanPolicy policy, string ns, string setName, string[] binNames, ulong taskId)
		{
			Begin();
			int fieldCount = 0;

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

			// Estimate scan options size.
			dataOffset += 2 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate scan timeout size.
			dataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate taskId size.
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			if (binNames != null)
			{
				foreach (String binName in binNames)
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
			WriteHeader(policy, readAttr, 0, fieldCount, operationCount);

			if (ns != null)
			{
				WriteField(ns, FieldType.NAMESPACE);
			}

			if (setName != null)
			{
				WriteField(setName, FieldType.TABLE);
			}

			WriteFieldHeader(2, FieldType.SCAN_OPTIONS);
			byte priority = (byte)policy.priority;
			priority <<= 4;

			if (policy.failOnClusterChange)
			{
				priority |= 0x08;
			}
			
			dataBuffer[dataOffset++] = priority;
			dataBuffer[dataOffset++] = (byte)policy.scanPercent;

			// Write scan timeout
			WriteFieldHeader(4, FieldType.SCAN_TIMEOUT);
			dataOffset += ByteUtil.IntToBytes((uint)policy.socketTimeout, dataBuffer, dataOffset);

			// Write taskId field
			WriteFieldHeader(8, FieldType.TRAN_ID);
			dataOffset += ByteUtil.LongToBytes(taskId, dataBuffer, dataOffset);

			if (binNames != null)
			{
				foreach (String binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		protected internal void SetQuery(Policy policy, Statement statement, bool write)
		{
			byte[] functionArgBuffer = null;
			int fieldCount = 0;
			int filterSize = 0;
			int binNameSize = 0;

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
				// Estimate scan options size.
				dataOffset += 2 + FIELD_HEADER_SIZE;
				fieldCount++;

				// Estimate scan timeout size.
				dataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			PredExp[] predExp = statement.PredExp;
			int predSize = 0;

			if (predExp != null)
			{
				dataOffset += FIELD_HEADER_SIZE;
				predSize = PredExp.EstimateSize(predExp);
				dataOffset += predSize;
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

			if (statement.filter == null)
			{
				if (statement.binNames != null)
				{
					foreach (string binName in statement.binNames)
					{
						EstimateOperationSize(binName);
					}
				}
			}

			SizeBuffer();
			int operationCount = (statement.filter == null && statement.binNames != null) ? statement.binNames.Length : 0;

			if (write)
			{
				WriteHeader((WritePolicy)policy, Command.INFO1_READ, Command.INFO2_WRITE, fieldCount, operationCount);
			}
			else
			{
				QueryPolicy qp = (QueryPolicy)policy;
				int readAttr = qp.includeBinData ? Command.INFO1_READ : Command.INFO1_READ | Command.INFO1_NOBINDATA;
				WriteHeader(policy, readAttr, 0, fieldCount, operationCount);
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
			WriteFieldHeader(8, FieldType.TRAN_ID);
			ByteUtil.LongToBytes(statement.taskId, dataBuffer, dataOffset);
			dataOffset += 8;

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
				WriteFieldHeader(2, FieldType.SCAN_OPTIONS);
				byte priority = (byte)policy.priority;
				priority <<= 4;

				if (!write && ((QueryPolicy)policy).failOnClusterChange)
				{
					priority |= 0x08;
				}

				dataBuffer[dataOffset++] = priority;
				dataBuffer[dataOffset++] = (byte)100;

				// Write scan timeout
				WriteFieldHeader(4, FieldType.SCAN_TIMEOUT);
				dataOffset += ByteUtil.IntToBytes((uint)policy.socketTimeout, dataBuffer, dataOffset);
			}

			if (predExp != null)
			{
				WriteFieldHeader(predSize, FieldType.PREDEXP);
				dataOffset = PredExp.Write(predExp, dataBuffer, dataOffset);
			}

			if (statement.functionName != null)
			{
				WriteFieldHeader(1, FieldType.UDF_OP);
				dataBuffer[dataOffset++] = (statement.returnData) ? (byte)1 : (byte)2;
				WriteField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
				WriteField(statement.functionName, FieldType.UDF_FUNCTION);
				WriteField(functionArgBuffer, FieldType.UDF_ARGLIST);
			}

			// Scan bin names are specified after all fields.
			if (statement.filter == null)
			{
				if (statement.binNames != null)
				{
					foreach (string binName in statement.binNames)
					{
						WriteOperation(binName, Operation.Type.READ);
					}
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
		/// Header write for write operations.
		/// </summary>
		private void WriteHeader(WritePolicy policy, int readAttr, int writeAttr, int fieldCount, int operationCount)
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
			dataOffset += ByteUtil.IntToBytes((uint)policy.totalTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Generic header write.
		/// </summary>
		private void WriteHeader(Policy policy, int readAttr, int writeAttr, int fieldCount, int operationCount)
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
			dataBuffer[dataOffset++] = (byte)writeAttr;
			dataBuffer[dataOffset++] = (byte)infoAttr;

			for (int i = 0; i < 10; i++)
			{
				dataBuffer[dataOffset++] = 0;
			}
			dataOffset += ByteUtil.IntToBytes((uint)policy.totalTimeout, dataBuffer, dataOffset);
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

		private void WriteFieldHeader(int size, int type)
		{
			ByteUtil.IntToBytes((uint)size+1, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = (byte)type;
		}

		private void Begin()
		{
			dataOffset = MSG_TOTAL_HEADER_SIZE;
		}

		protected internal abstract void SizeBuffer();
		protected internal abstract void End();
	}
}
