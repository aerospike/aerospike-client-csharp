/* 
 * Copyright 2012-2015 Aerospike, Inc.
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

namespace Aerospike.Client
{
	public abstract class Command
	{
		// Flags commented out are not supported by this client.
		public static readonly int INFO1_READ            = (1 << 0); // Contains a read operation.
		public static readonly int INFO1_GET_ALL         = (1 << 1); // Get all bins.
		public static readonly int INFO1_NOBINDATA       = (1 << 5); // Do not read the bins.
		public static readonly int INFO1_CONSISTENCY_ALL = (1 << 6); // Involve all replicas in read operation.

		public static readonly int INFO2_WRITE          = (1 << 0); // Create or update record
		public static readonly int INFO2_DELETE         = (1 << 1); // Fling a record into the belly of Moloch.
		public static readonly int INFO2_GENERATION     = (1 << 2); // Update if expected generation == old.
		public static readonly int INFO2_GENERATION_GT  = (1 << 3); // Update if new generation >= old, good for restore.
		public static readonly int INFO2_GENERATION_DUP = (1 << 4); // Create a duplicate on a generation collision.
		public static readonly int INFO2_CREATE_ONLY    = (1 << 5); // Create only. Fail if record already exists.

		public static readonly int INFO3_LAST              = (1 << 0); // This is the last of a multi-part message.
		public static readonly int INFO3_COMMIT_MASTER     = (1 << 1); // Commit to master only before declaring success.
		public static readonly int INFO3_UPDATE_ONLY       = (1 << 3); // Update only. Merge bins.
		public static readonly int INFO3_CREATE_OR_REPLACE = (1 << 4); // Create or completely replace record.
		public static readonly int INFO3_REPLACE_ONLY      = (1 << 5); // Completely replace existing record only.

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

		public void SetOperate(WritePolicy policy, Key key, Operation[] operations)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);
			int readAttr = 0;
			int writeAttr = 0;
			bool readBin = false;
			bool readHeader = false;

			foreach (Operation operation in operations)
			{
				switch (operation.type)
				{
				case Operation.Type.READ:
					readAttr |= Command.INFO1_READ;

					// Read all bins if no bin is specified.
					if (operation.binName == null)
					{
						readAttr |= Command.INFO1_GET_ALL;
					}
					readBin = true;
					break;

				case Operation.Type.READ_HEADER:
					readAttr |= Command.INFO1_READ;
					readHeader = true;
					break;

				default:
					writeAttr = Command.INFO2_WRITE;
					break;
				}
				EstimateOperationSize(operation);
			}
			SizeBuffer();

			if (readHeader && !readBin)
			{
				readAttr |= Command.INFO1_NOBINDATA;
			}

			WriteHeader(policy, readAttr, writeAttr, fieldCount, operations.Length);
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

		public void SetBatchExists(Policy policy, Key[] keys)
		{
			// Estimate buffer size
			string ns = keys[0].ns;
			Begin();
			int byteSize = keys.Length * Command.DIGEST_SIZE;

			dataOffset += ByteUtil.EstimateSizeUtf8(ns) + FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;

			SizeBuffer();

			WriteHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, 2, 0);
			WriteField(ns, FieldType.NAMESPACE);
			WriteFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);

			foreach (Key key in keys)
			{
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;
			}
			End();
		}

		public void SetBatchExists(Policy policy, Key[] keys, BatchNode.BatchNamespace batch)
		{
			// Estimate buffer size
			Begin();
			int byteSize = batch.offsetsSize * SyncCommand.DIGEST_SIZE;

			dataOffset +=  ByteUtil.EstimateSizeUtf8(batch.ns) + FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
    
			SizeBuffer();
    
			WriteHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, 2, 0);
			WriteField(batch.ns, FieldType.NAMESPACE);
			WriteFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);

			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;
			}
			End();
		}

		public void SetBatchGet(Policy policy, Key[] keys, HashSet<string> binNames, int readAttr)
		{
			// Estimate buffer size
			string ns = keys[0].ns;
			Begin();
			int byteSize = keys.Length * SyncCommand.DIGEST_SIZE;

			dataOffset += ByteUtil.EstimateSizeUtf8(ns) + FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
			}

			SizeBuffer();

			int operationCount = (binNames == null) ? 0 : binNames.Count;
			WriteHeader(policy, readAttr, 0, 2, operationCount);
			WriteField(ns, FieldType.NAMESPACE);
			WriteFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);

			foreach (Key key in keys)
			{
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;
			}

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		public void SetBatchGet(Policy policy, Key[] keys, BatchNode.BatchNamespace batch, HashSet<string> binNames, int readAttr)
		{
			// Estimate buffer size
			Begin();
			int byteSize = batch.offsetsSize * SyncCommand.DIGEST_SIZE;

			dataOffset +=  ByteUtil.EstimateSizeUtf8(batch.ns) + FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
			}

			SizeBuffer();

			int operationCount = (binNames == null)? 0 : binNames.Count;
			WriteHeader(policy, readAttr, 0, 2, operationCount);
			WriteField(batch.ns, FieldType.NAMESPACE);
			WriteFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);

			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;
			}

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		public void SetScan(ScanPolicy policy, string ns, string setName, string[] binNames, long taskId)
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

			// Write taskId field
			WriteFieldHeader(8, FieldType.TRAN_ID);
			ByteUtil.LongToBytes((ulong)taskId, dataBuffer, dataOffset);
			dataOffset += 8;

			if (binNames != null)
			{
				foreach (String binName in binNames)
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
				dataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE;
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
			dataOffset += operation.binValue.EstimateSize();
		}

		protected void EstimateOperationSize(string binName)
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
		protected internal void WriteHeader(WritePolicy policy, int readAttr, int writeAttr, int fieldCount, int operationCount)
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

			if (policy.consistencyLevel == ConsistencyLevel.CONSISTENCY_ALL)
			{
				readAttr |= Command.INFO1_CONSISTENCY_ALL;
			}

			// Write all header data except total size which must be written last. 
			dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[9] = (byte)readAttr;
			dataBuffer[10] = (byte)writeAttr;
			dataBuffer[11] = (byte)infoAttr;
			dataBuffer[12] = 0; // unused
			dataBuffer[13] = 0; // clear the result code
			ByteUtil.IntToBytes((uint)generation, dataBuffer, 14);
			ByteUtil.IntToBytes((uint)policy.expiration, dataBuffer, 18);

			// Initialize timeout. It will be written later.
			dataBuffer[22] = 0;
			dataBuffer[23] = 0;
			dataBuffer[24] = 0;
			dataBuffer[25] = 0;

			ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, 26);
			ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, 28);
			dataOffset = MSG_TOTAL_HEADER_SIZE;
		}

		/// <summary>
		/// Generic header write.
		/// </summary>
		protected internal void WriteHeader(Policy policy, int readAttr, int writeAttr, int fieldCount, int operationCount)
		{
			if (policy.consistencyLevel == ConsistencyLevel.CONSISTENCY_ALL)
			{
				readAttr |= Command.INFO1_CONSISTENCY_ALL;
			}

			// Write all header data except total size which must be written last. 
			dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[9] = (byte)readAttr;
			dataBuffer[10] = (byte)writeAttr;

			for (int i = 11; i < 26; i++)
			{
				dataBuffer[i] = 0;
			}
			ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, 26);
			ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, 28);
			dataOffset = MSG_TOTAL_HEADER_SIZE;
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
			int valueLength = operation.binValue.Write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operation.type);
			dataBuffer[dataOffset++] = (byte) operation.binValue.Type;
			dataBuffer[dataOffset++] = (byte) 0;
			dataBuffer[dataOffset++] = (byte) nameLength;
			dataOffset += nameLength + valueLength;
		}

		protected void WriteOperation(string name, Operation.Type operationType)
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

		public void WriteField(Value value, int type)
		{
			int offset = dataOffset + FIELD_HEADER_SIZE;
			dataBuffer[offset++] = (byte)value.Type;
			int len = value.Write(dataBuffer, offset) + 1;
			WriteFieldHeader(len, type);
			dataOffset += len;
		}
		
		public void WriteField(string str, int type)
		{
			int len = ByteUtil.StringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
			WriteFieldHeader(len, type);
			dataOffset += len;
		}

		public void WriteField(byte[] bytes, int type)
		{
			Array.Copy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.Length);
			WriteFieldHeader(bytes.Length, type);
			dataOffset += bytes.Length;
		}

		public void WriteFieldHeader(int size, int type)
		{
			ByteUtil.IntToBytes((uint)size+1, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = (byte)type;
		}

		protected internal void Begin()
		{
			dataOffset = MSG_TOTAL_HEADER_SIZE;
		}

		protected internal void End()
		{
			// Write total size of message which is the current offset.
			ulong size = ((ulong)dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);
		}

		protected internal abstract Policy GetPolicy();
		protected internal abstract void WriteBuffer();
		protected internal abstract void SizeBuffer();
	}
}
