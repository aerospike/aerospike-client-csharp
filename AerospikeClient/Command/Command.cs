/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
using System.Collections;
using Aerospike.Client.Config;

#pragma warning disable 0618

namespace Aerospike.Client
{
	public abstract class Command
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
		public static readonly int INFO2_DURABLE_DELETE = (1 << 4); // Command resulting in record deletion leaves tombstone (Enterprise only).
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

		public static readonly int INFO4_TXN_VERIFY_READ = (1 << 0); // Send transaction version to the server to be verified.
		public static readonly int INFO4_TXN_ROLL_FORWARD = (1 << 1); // Roll forward transaction.
		public static readonly int INFO4_TXN_ROLL_BACK = (1 << 2); // Roll back transaction.
		public static readonly int INFO4_TXN_ON_LOCKING_ONLY = (1 << 4); // Must be able to lock record in transaction.

		public const byte STATE_READ_AUTH_HEADER = 1;
		public const byte STATE_READ_HEADER = 2;
		public const byte STATE_READ_DETAIL = 3;
		public const byte STATE_COMPLETE = 4;

		public const byte BATCH_MSG_READ = 0x0;
		public const byte BATCH_MSG_REPEAT = 0x1;
		public const byte BATCH_MSG_INFO = 0x2;
		public const byte BATCH_MSG_GEN = 0x4;
		public const byte BATCH_MSG_TTL = 0x8;
		public const byte BATCH_MSG_INFO4 = 0x10;

		public const int MSG_TOTAL_HEADER_SIZE = 30;
		public const int FIELD_HEADER_SIZE = 5;
		public const int OPERATION_HEADER_SIZE = 8;
		public const int MSG_REMAINING_HEADER_SIZE = 22;
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
		internal long? Version;

		protected int resultCode;
		protected int generation;
		protected int expiration;
		protected int fieldCount;
		protected int opCount;

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

			resultCode = 0;
			generation = 0;
			expiration = 0;
			fieldCount = 0;
			opCount = 0;
		}

		//--------------------------------------------------
		// Transaction
		//--------------------------------------------------

		public void SetTxnAddKeys(WritePolicy policy, Key key, OperateArgs args)
		{
			Begin();
			int fieldCount = EstimateKeySize(key);
			dataOffset += args.size;

			bool compress = SizeBuffer(policy);

			dataOffset += 8;
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE;
			dataBuffer[dataOffset++] = (byte)args.readAttr;
			dataBuffer[dataOffset++] = (byte)args.writeAttr;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = 0;
			dataBuffer[dataOffset++] = 0;
			dataOffset += ByteUtil.IntToBytes(0, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)policy.expiration, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)args.operations.Length, dataBuffer, dataOffset);

			WriteKey(key);

			foreach (Operation operation in args.operations)
			{
				WriteOperation(operation);
			}

			End(compress);
		}

		public void SetTxnVerify(Key key, long ver)
		{
			Begin();
			int fieldCount = EstimateKeySize(key);

			// Version field.
			dataOffset += 7 + FIELD_HEADER_SIZE;
			fieldCount++;

			SizeBuffer();
			dataOffset += 8;
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE;
			dataBuffer[dataOffset++] = (byte)(Command.INFO1_READ | Command.INFO1_NOBINDATA);
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)Command.INFO3_SC_READ_TYPE;
			dataBuffer[dataOffset++] = (byte)Command.INFO4_TXN_VERIFY_READ;
			dataBuffer[dataOffset++] = 0;
			dataOffset += ByteUtil.IntToBytes(0, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes(0, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes(0, dataBuffer, dataOffset);

			WriteKey(key);
			WriteFieldVersion(ver);
			End();
		}

		public void SetBatchTxnVerify(
			BatchPolicy policy,
			Key[] keys,
			long?[] versions,
			BatchNode batch
		)
		{
			BatchOffsetsNative offsets = new(batch);
			SetBatchTxnVerify(policy, keys, versions, offsets);
		}

		public void SetBatchTxnVerify(
			BatchPolicy policy,
			Key[] keys,
			long?[] versions,
			BatchOffsets offsets
		)
		{
			// Estimate buffer size.
			Begin();

			// Batch field
			dataOffset += FIELD_HEADER_SIZE + 5;

			Key keyPrev = null;
			long? verPrev = null;
			int max = offsets.Size();

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions[offset];

				dataOffset += key.digest.Length + 4;

				if (CanRepeat(key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					dataOffset += 9; // header(4) + info4(1) + fieldCount(2) + opCount(2) = 9
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (ver.HasValue)
					{
						dataOffset += 7 + FIELD_HEADER_SIZE;
					}
					keyPrev = key;
					verPrev = ver;
				}
			}

			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, 1);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);
			keyPrev = null;
			verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions[offset];

				ByteUtil.IntToBytes((uint)offset, dataBuffer, dataOffset);
				dataOffset += 4;

				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				if (CanRepeat(key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4);
					dataBuffer[dataOffset++] = (byte)(Command.INFO1_READ | Command.INFO1_NOBINDATA);
					dataBuffer[dataOffset++] = (byte)0;
					dataBuffer[dataOffset++] = (byte)Command.INFO3_SC_READ_TYPE;
					dataBuffer[dataOffset++] = (byte)Command.INFO4_TXN_VERIFY_READ;

					int fieldCount = 0;

					if (ver.HasValue)
					{
						fieldCount++;
					}

					WriteBatchFields(key, fieldCount, 0);

					if (ver.HasValue)
					{
						WriteFieldVersion(ver.Value);
					}

					keyPrev = key;
					verPrev = ver;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
			End(compress);
		}

		public void SetTxnMarkRollForward(Key key)
		{
			Bin bin = new("fwd", true);

			Begin();
			int fieldCount = EstimateKeySize(key);
			EstimateOperationSize(bin);
			//SizeBuffer();
			WriteTxnMonitor(key, 0, Command.INFO2_WRITE, fieldCount, 1);
			WriteOperation(bin, Operation.Type.WRITE);
			End();
		}

		public void SetTxnRoll(Key key, Txn txn, int txnAttr)
		{
			Begin();
			int fieldCount = EstimateKeySize(key);

			fieldCount += SizeTxn(key, txn, false);

			SizeBuffer();
			dataOffset += 8;
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)(Command.INFO2_WRITE | Command.INFO2_DURABLE_DELETE);
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)txnAttr;
			dataBuffer[dataOffset++] = 0; // clear the result code
			dataOffset += ByteUtil.IntToBytes(0, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes(0, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes(0, dataBuffer, dataOffset);

			WriteKey(key);
			WriteTxn(txn, false);
			End();
		}

		public void SetBatchTxnRoll(
			BatchPolicy policy,
			Txn txn,
			Key[] keys,
			BatchNode batch,
			BatchAttr attr
		)
		{
			BatchOffsetsNative offsets = new(batch);
			SetBatchTxnRoll(policy, txn, keys, attr, offsets);
		}

		public void SetBatchTxnRoll(
			BatchPolicy policy,
			Txn txn,
			Key[] keys,
			BatchAttr attr,
			BatchOffsets offsets
		)
		{
			// Estimate buffer size.
			Begin();
			int fieldCount = 1;
			int max = offsets.Size();
			long?[] versions = new long?[max];

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				versions[i] = txn.GetReadVersion(key);
			}

			// Batch field
			dataOffset += FIELD_HEADER_SIZE + 5;

			Key keyPrev = null;
			long? verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions[i];

				dataOffset += key.digest.Length + 4;

				if (CanRepeat(key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					dataOffset += 12; // header(4) + ttl(4) + fieldCount(2) + opCount(2) = 12
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					SizeTxnBatch(txn, ver, attr.hasWrite);
					dataOffset += 2; // gen(2) = 2
					keyPrev = key;
					verPrev = ver;
				}
			}

			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, fieldCount);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);
			keyPrev = null;
			verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions[i];

				ByteUtil.IntToBytes((uint)offset, dataBuffer, dataOffset);
				dataOffset += 4;

				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				if (CanRepeat(key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					WriteBatchWrite(key, txn, ver, attr, null, 0, 0);
					keyPrev = key;
					verPrev = ver;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
			End(compress);
		}

		public void SetTxnClose(Txn txn, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(key);
			//SizeBuffer();
			WriteTxnMonitor(key, 0, Command.INFO2_WRITE | Command.INFO2_DELETE | Command.INFO2_DURABLE_DELETE,
				fieldCount, 0);
			End();
		}

		private void WriteTxnMonitor(Key key, int readAttr, int writeAttr, int fieldCount, int opCount)
		{
			SizeBuffer();
			dataOffset += 8;
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE;
			dataBuffer[dataOffset++] = (byte)readAttr;
			dataBuffer[dataOffset++] = (byte)writeAttr;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = 0;
			dataBuffer[dataOffset++] = 0;
			dataOffset += ByteUtil.IntToBytes(0, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes(0, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)opCount, dataBuffer, dataOffset);

			WriteKey(key);
		}

		//--------------------------------------------------
		// Writes
		//--------------------------------------------------

		public virtual void SetWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key, true);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			foreach (Bin bin in bins)
			{
				EstimateOperationSize(bin);
			}

			bool compress = SizeBuffer(policy);

			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, bins.Length);
			WriteKey(policy, key, true);

			policy.filterExp?.Write(this);

			foreach (Bin bin in bins)
			{
				WriteOperation(bin, operation);
			}
			End(compress);
		}

		public virtual void SetDelete(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key, true);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderWrite(policy, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
			WriteKey(policy, key, true);

			policy.filterExp?.Write(this);
			End();
		}

		public void SetDelete(Policy policy, Key key, BatchAttr attr)
		{
			Begin();
			Expression exp = GetBatchExpression(policy, attr);
			int fieldCount = EstimateKeyAttrSize(policy, key, attr, exp);
			SizeBuffer();
			WriteKeyAttr(policy, key, attr, exp, fieldCount, 0);
			End();
		}

		public virtual void SetTouch(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key, true);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			EstimateOperationSize();
			SizeBuffer();
			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 1);
			WriteKey(policy, key, true);

			policy.filterExp?.Write(this);
			WriteOperation(Operation.Type.TOUCH);
			End();
		}

		//--------------------------------------------------
		// Reads
		//--------------------------------------------------

		public virtual void SetExists(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key, false);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(policy, key, false);

			policy.filterExp?.Write(this);
			End();
		}

		public virtual void SetRead(Policy policy, Key key, string[] binNames)
		{
			int readAttr = Command.INFO1_READ;
			int opCount = 0;

			if (binNames != null && binNames.Length > 0)
			{
				opCount = binNames.Length;
			}
			else
			{
				readAttr |= Command.INFO1_GET_ALL;
			}

			Begin();
			int fieldCount = EstimateKeySize(policy, key, false);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			if (opCount != 0)
			{
				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
			}

			SizeBuffer();
			WriteHeaderRead(policy, serverTimeout, readAttr, 0, 0, fieldCount, opCount);
			WriteKey(policy, key, false);

			policy.filterExp?.Write(this);

			if (opCount != 0)
			{
				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		public void SetRead(Policy policy, BatchRead br)
		{
			Begin();

			BatchReadPolicy rp = br.policy;
			BatchAttr attr = new();
			Expression exp;
			int opCount;

			if (rp != null)
			{
				attr.SetRead(rp);
				exp = rp.filterExp ?? policy.filterExp;
			}
			else
			{
				attr.SetRead(policy);
				exp = policy.filterExp;
			}

			if (br.binNames != null)
			{
				opCount = br.binNames.Length;

				foreach (string binName in br.binNames)
				{
					EstimateOperationSize(binName);
				}
			}
			else if (br.ops != null)
			{
				attr.AdjustRead(br.ops);
				opCount = br.ops.Length;

				foreach (Operation op in br.ops)
				{
					if (Operation.IsWrite(op.type))
					{
						throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in read");
					}
					EstimateOperationSize(op);
				}
			}
			else
			{
				attr.AdjustRead(br.readAllBins);
				opCount = 0;
			}

			int fieldCount = EstimateKeyAttrSize(policy, br.key, attr, exp);

			SizeBuffer();
			WriteKeyAttr(policy, br.key, attr, exp, fieldCount, opCount);

			if (br.binNames != null)
			{
				foreach (string binName in br.binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			else if (br.ops != null)
			{
				foreach (Operation op in br.ops)
				{
					WriteOperation(op);
				}
			}
			End();
		}

		public void SetRead(Policy policy, Key key, Operation[] ops)
		{
			Begin();

			BatchAttr attr = new();
			attr.SetRead(policy);
			attr.AdjustRead(ops);

			int fieldCount = EstimateKeyAttrSize(policy, key, attr, policy.filterExp);

			foreach (Operation op in ops)
			{
				if (Operation.IsWrite(op.type))
				{
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in read");
				}
				EstimateOperationSize(op);
			}

			SizeBuffer();
			WriteKeyAttr(policy, key, attr, policy.filterExp, fieldCount, ops.Length);

			foreach (Operation op in ops)
			{
				WriteOperation(op);
			}
			End();
		}

		public virtual void SetReadHeader(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key, false);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(policy, key, false);

			policy.filterExp?.Write(this);
			End();
		}

		//--------------------------------------------------
		// Operate
		//--------------------------------------------------

		public virtual void SetOperate(WritePolicy policy, Key key, OperateArgs args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key, args.hasWrite);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			dataOffset += args.size;

			bool compress = SizeBuffer(policy);

			WriteHeaderReadWrite(policy, args, fieldCount);
			WriteKey(policy, key, args.hasWrite);

			policy.filterExp?.Write(this);

			foreach (Operation operation in args.operations)
			{
				WriteOperation(operation);
			}
			End(compress);
		}

		public void SetOperate(Policy policy, BatchAttr attr, Key key, Operation[] ops)
		{
			Begin();
			Expression exp = GetBatchExpression(policy, attr);
			int fieldCount = EstimateKeyAttrSize(policy, key, attr, exp);

			dataOffset += attr.opSize;
			bool compress = SizeBuffer(policy);
			WriteKeyAttr(policy, key, attr, exp, fieldCount, ops.Length);

			foreach (Operation op in ops)
			{
				WriteOperation(op);
			}
			End(compress);
		}


		//--------------------------------------------------
		// UDF
		//--------------------------------------------------

		public virtual void SetUdf(WritePolicy policy, Key key, string packageName, string functionName, Value[] args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key, true);

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			byte[] argBytes = Packer.Pack(args);
			fieldCount += EstimateUdfSize(packageName, functionName, argBytes);

			bool compress = SizeBuffer(policy);

			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 0);
			WriteKey(policy, key, true);

			policy.filterExp?.Write(this);
			WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
			WriteField(functionName, FieldType.UDF_FUNCTION);
			WriteField(argBytes, FieldType.UDF_ARGLIST);
			End(compress);
		}

		public void SetUdf(Policy policy, BatchAttr attr, Key key, string packageName, string functionName, Value[] args)
		{
			byte[] argBytes = Packer.Pack(args);
			SetUdf(policy, attr, key, packageName, functionName, argBytes);
		}

		public void SetUdf(Policy policy, BatchAttr attr, Key key, string packageName, string functionName, byte[] argBytes)
		{
			Begin();
			Expression exp = GetBatchExpression(policy, attr);
			int fieldCount = EstimateKeyAttrSize(policy, key, attr, exp);
			fieldCount += EstimateUdfSize(packageName, functionName, argBytes);

			bool compress = SizeBuffer(policy);
			WriteKeyAttr(policy, key, attr, exp, fieldCount, 0);
			WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
			WriteField(functionName, FieldType.UDF_FUNCTION);
			WriteField(argBytes, FieldType.UDF_ARGLIST);
			End(compress);
		}

		//--------------------------------------------------
		// Batch Read Only
		//--------------------------------------------------

		public virtual void SetBatchRead(BatchPolicy policy, List<BatchRead> records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			BatchRead prev = null;

			Begin();
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
							EstimateOperationSize(binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							EstimateReadOperationSize(op);
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

			WriteHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, 0, 0, fieldCount, 0);

			policy.filterExp?.Write(this);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

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
						WriteBatchFields(key, 0, binNames.Length);

						foreach (string binName in binNames)
						{
							WriteOperation(binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = dataOffset++;
						WriteBatchFields(key, 0, ops.Length);
						dataBuffer[offset] = (byte)WriteReadOnlyOperations(ops, readAttr);
					}
					else
					{
						dataBuffer[dataOffset++] = (byte)(readAttr | (record.readAllBins ? Command.INFO1_GET_ALL : Command.INFO1_NOBINDATA));
						WriteBatchFields(key, 0, 0);
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(compress);
		}

		public virtual void SetBatchRead
		(
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
			Begin();
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
							EstimateOperationSize(binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							EstimateReadOperationSize(op);
						}
					}
					prev = key;
				}
			}

			bool compress = SizeBuffer(policy);

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			WriteHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, 0, 0, fieldCount, 0);

			policy.filterExp?.Write(this);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

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
						WriteBatchFields(key, 0, binNames.Length);

						foreach (String binName in binNames)
						{
							WriteOperation(binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = dataOffset++;
						WriteBatchFields(key, 0, ops.Length);
						dataBuffer[offset] = (byte)WriteReadOnlyOperations(ops, readAttr);
					}
					else
					{
						dataBuffer[dataOffset++] = (byte)readAttr;
						WriteBatchFields(key, 0, 0);
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(compress);
		}

		//--------------------------------------------------
		// Batch Read/Write Operations
		//--------------------------------------------------

		public virtual void SetBatchOperate(
			BatchPolicy policy,
			IList records,
			BatchNode batch,
			IConfigProvider configProvider)
		{
			BatchOffsetsNative offsets = new(batch);
			SetBatchOperate(policy, records, offsets, configProvider);
		}

		public void SetBatchOperate(
			BatchPolicy policy,
			IList records,
			BatchOffsets offsets,
			IConfigProvider configProvider)
		{
			Begin();
			int max = offsets.Size();
			Txn txn = policy.Txn;
			long?[] versions = null;

			if (txn != null)
			{
				versions = new long?[max];

				for (int i = 0; i < max; i++)
				{
					int offset = offsets.Get(i);
					BatchRecord record = (BatchRecord)records[offset];
					versions[i] = txn.GetReadVersion(record.key);
				}
			}

			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				dataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			BatchRecord prev = null;
			long? verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				BatchRecord record = (BatchRecord)records[offset];
				Key key = record.key;
				long? ver = versions?[i];

				dataOffset += key.digest.Length + 4;

				if (CanRepeat(policy, key, record, prev, ver, verPrev, configProvider))
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
					SizeTxnBatch(txn, ver, record.hasWrite);
					dataOffset += record.Size(policy, configProvider);
					prev = record;
					verPrev = ver;
				}
			}
			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, fieldCount);

			policy.filterExp?.Write(this);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);

			BatchAttr attr = new();
			prev = null;
			verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				BatchRecord record = (BatchRecord)records[offset];
				long? ver = versions?[i];
				ByteUtil.IntToBytes((uint)offset, dataBuffer, dataOffset);
				dataOffset += 4;

				Key key = record.key;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				if (CanRepeat(policy, key, record, prev, ver, verPrev, configProvider))
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
									if (br.binNames.Length > 0)
									{
										WriteBatchBinNames(key, txn, ver, br.binNames, attr, attr.filterExp);
									}
									else
									{
										attr.AdjustRead(true);
										WriteBatchRead(key, txn, ver, attr, attr.filterExp, 0);
									}
								}
								else if (br.ops != null)
								{
									attr.AdjustRead(br.ops);
									WriteBatchOperations(key, txn, ver, br.ops, attr, attr.filterExp);
								}
								else
								{
									attr.AdjustRead(br.readAllBins);
									WriteBatchRead(key, txn, ver, attr, attr.filterExp, 0);
								}
								break;
							}

						case BatchRecord.Type.BATCH_WRITE:
							{
								BatchWrite bw = (BatchWrite)record;

								if (bw.policy != null)
								{
									if (configProvider != null && configProvider.ConfigurationData != null)
									{
										if (configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.HasValue)
										{
											bw.policy.sendKey = configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.Value;
										}
										if (configProvider.ConfigurationData.dynamicConfig.batch_write.durable_delete.HasValue)
										{
											bw.policy.durableDelete = configProvider.ConfigurationData.dynamicConfig.batch_write.durable_delete.Value;
										}
									}

									attr.SetWrite(bw.policy);
								}
								else
								{
									if (configProvider != null && configProvider.ConfigurationData != null)
									{
										if (configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.HasValue)
										{
											policy.sendKey = configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.Value;
										}
									}

									attr.SetWrite(policy);
								}
								attr.AdjustWrite(bw.ops);
								WriteBatchOperations(key, txn, ver, bw.ops, attr, attr.filterExp);
								break;
							}

						case BatchRecord.Type.BATCH_UDF:
							{
								BatchUDF bu = (BatchUDF)record;

								if (bu.policy != null)
								{
									if (configProvider != null && configProvider.ConfigurationData != null)
									{
										if (configProvider.ConfigurationData.dynamicConfig.batch_udf.send_key.HasValue)
										{
											bu.policy.sendKey = configProvider.ConfigurationData.dynamicConfig.batch_udf.send_key.Value;
										}
										if (configProvider.ConfigurationData.dynamicConfig.batch_udf.durable_delete.HasValue)
										{
											bu.policy.durableDelete = configProvider.ConfigurationData.dynamicConfig.batch_udf.durable_delete.Value;
										}
									}

									attr.SetUDF(bu.policy);
								}
								else
								{
									if (configProvider != null && configProvider.ConfigurationData != null)
									{
										if (configProvider.ConfigurationData.dynamicConfig.batch_udf.send_key.HasValue)
										{
											policy.sendKey = configProvider.ConfigurationData.dynamicConfig.batch_udf.send_key.Value;
										}
									}

									attr.SetUDF(policy);
								}
								WriteBatchWrite(key, txn, ver, attr, attr.filterExp, 3, 0);
								WriteField(bu.packageName, FieldType.UDF_PACKAGE_NAME);
								WriteField(bu.functionName, FieldType.UDF_FUNCTION);
								WriteField(bu.argBytes, FieldType.UDF_ARGLIST);
								break;
							}

						case BatchRecord.Type.BATCH_DELETE:
							{
								BatchDelete bd = (BatchDelete)record;

								if (bd.policy != null)
								{
									if (configProvider != null && configProvider.ConfigurationData != null)
									{
										if (configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.HasValue)
										{
											bd.policy.sendKey = configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.Value;
										}
										if (configProvider.ConfigurationData.dynamicConfig.batch_delete.durable_delete.HasValue)
										{
											bd.policy.durableDelete = configProvider.ConfigurationData.dynamicConfig.batch_delete.durable_delete.Value;
										}
									}

									attr.SetDelete(bd.policy);
								}
								else
								{
									if (configProvider != null && configProvider.ConfigurationData != null)
									{
										if (configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.HasValue)
										{
											policy.sendKey = configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.Value;
										}
									}

									attr.SetDelete(policy);
								}
								WriteBatchWrite(key, txn, ver, attr, attr.filterExp, 0, 0);
								break;
							}
					}
					prev = record;
					verPrev = ver;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(dataOffset - MSG_TOTAL_HEADER_SIZE - 4), dataBuffer, fieldSizeOffset);
			End(compress);
		}

		public virtual void SetBatchOperate
		(
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string[] binNames,
			Operation[] ops,
			BatchAttr attr
		)
		{
			BatchOffsetsNative offsets = new(batch);
			SetBatchOperate(policy, keys, binNames, ops, attr, offsets);
		}

		public void SetBatchOperate(
			BatchPolicy policy,
			Key[] keys,
			string[] binNames,
			Operation[] ops,
			BatchAttr attr,
			BatchOffsets offsets
		)
		{
			// Estimate full row size
			int max = offsets.Size();
			Txn txn = policy.Txn;
			long?[] versions = null;

			Begin();

			if (txn != null)
			{
				versions = new long?[max];

				for (int i = 0; i < max; i++)
				{
					int offset = offsets.Get(i);
					Key key = keys[offset];
					versions[i] = txn.GetReadVersion(key);
				}
			}

			Expression exp = GetBatchExpression(policy, attr);
			int fieldCount = 1;

			if (exp != null)
			{
				dataOffset += exp.Size();
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			Key keyPrev = null;
			long? verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions?[i];

				dataOffset += key.digest.Length + 4;

				if (CanRepeat(attr, key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					dataOffset += 12; // header(4) + ttl(4) + fieldCount(2) + opCount(2) = 12
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					SizeTxnBatch(txn, ver, attr.hasWrite);

					if (attr.sendKey)
					{
						dataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							EstimateOperationSize(binName);
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
							EstimateOperationSize(op);
						}
					}
					else if ((attr.writeAttr & Command.INFO2_DELETE) != 0)
					{
						dataOffset += 2; // Extra write specific fields.
					}
					keyPrev = key;
					verPrev = ver;
				}
			}

			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, fieldCount);

			exp?.Write(this);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);
			keyPrev = null;
			verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions?[i];

				ByteUtil.IntToBytes((uint)offset, dataBuffer, dataOffset);
				dataOffset += 4;

				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				if (CanRepeat(attr, key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					if (binNames != null)
					{
						WriteBatchBinNames(key, txn, ver, binNames, attr, null);
					}
					else if (ops != null)
					{
						WriteBatchOperations(key, txn, ver, ops, attr, null);
					}
					else if ((attr.writeAttr & Command.INFO2_DELETE) != 0)
					{
						WriteBatchWrite(key, txn, ver, attr, null, 0, 0);
					}
					else
					{
						WriteBatchRead(key, txn, ver, attr, null, 0);
					}
					keyPrev = key;
					verPrev = ver;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
			End(compress);
		}

		public virtual void SetBatchUDF(
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchAttr attr
		)
		{
			BatchOffsetsNative offsets = new(batch);
			SetBatchUDF(policy, keys, packageName, functionName, argBytes, attr, offsets);
		}

		public virtual void SetBatchUDF
		(
			BatchPolicy policy,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchAttr attr,
			BatchOffsets offsets
		)
		{
			// Estimate buffer size.
			Begin();
			int max = offsets.Size();
			Txn txn = policy.Txn;
			long?[] versions = null;

			if (txn != null)
			{
				versions = new long?[max];

				for (int i = 0; i < max; i++)
				{
					int offset = offsets.Get(i);
					Key key = keys[offset];
					versions[i] = txn.GetReadVersion(key);
				}
			}

			Expression exp = GetBatchExpression(policy, attr);
			int fieldCount = 1;

			if (exp != null)
			{
				dataOffset += exp.Size();
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE + 5;

			Key keyPrev = null;
			long? verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions?[i];

				dataOffset += key.digest.Length + 4;

				if (CanRepeat(attr, key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					dataOffset += 12; // header(4) + ttl(4) + fieldCount(2) + opCount(2) = 12
					dataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					dataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					SizeTxnBatch(txn, ver, attr.hasWrite);

					if (attr.sendKey)
					{
						dataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}
					dataOffset += 2; // gen(2) = 2
					EstimateUdfSize(packageName, functionName, argBytes);
					keyPrev = key;
					verPrev = ver;
				}
			}

			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, fieldCount);

			exp?.Write(this);

			int fieldSizeOffset = dataOffset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

			ByteUtil.IntToBytes((uint)max, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = GetBatchFlags(policy);
			keyPrev = null;
			verPrev = null;

			for (int i = 0; i < max; i++)
			{
				int offset = offsets.Get(i);
				Key key = keys[offset];
				long? ver = versions?[i];

				ByteUtil.IntToBytes((uint)offset, dataBuffer, dataOffset);
				dataOffset += 4;

				byte[] digest = key.digest;
				Array.Copy(digest, 0, dataBuffer, dataOffset, digest.Length);
				dataOffset += digest.Length;

				if (CanRepeat(attr, key, keyPrev, ver, verPrev))
				{
					// Can set repeat previous namespace/bin names to save space.
					dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					WriteBatchWrite(key, txn, ver, attr, null, 3, 0);
					WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
					WriteField(functionName, FieldType.UDF_FUNCTION);
					WriteField(argBytes, FieldType.UDF_ARGLIST);
					keyPrev = key;
					verPrev = ver;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
			End(compress);
		}

		private static bool CanRepeat(
			Policy policy,
			Key key,
			BatchRecord record,
			BatchRecord prev,
			long? ver,
			long? verPrev,
			IConfigProvider configProvider
		)
		{
			// Avoid relatively expensive full equality checks for performance reasons.
			// Use reference equality only in hope that common namespaces/bin names are set from
			// fixed variables.  It's fine if equality not determined correctly because it just
			// results in more space used. The batch will still be correct.
			// Same goes for ver reference equality check.
			if (!(verPrev == ver && prev != null && prev.key.ns == key.ns &&
				prev.key.setName == key.setName))
			{
				return false;
			}

			bool sendKey = policy.sendKey;
			if (configProvider != null && configProvider.ConfigurationData.HasDBWCsendKey())
			{
				sendKey = configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.Value;
			}

			if (sendKey)
			{
				return false;
			}

			return record.Equals(prev);
		}

		private static bool CanRepeat(BatchAttr attr, Key key, Key keyPrev, long? ver, long? verPrev)
		{
			return !attr.sendKey && verPrev == ver && keyPrev != null && keyPrev.ns == key.ns &&
					keyPrev.setName == key.setName;
		}

		private static bool CanRepeat(Key key, Key keyPrev, long? ver, long? verPrev)
		{
			return verPrev == ver && keyPrev != null && keyPrev.ns == key.ns &&
					keyPrev.setName == key.setName;
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

		private void SizeTxnBatch(Txn txn, long? ver, bool hasWrite)
		{
			if (txn != null)
			{
				dataOffset++; // Add info4 byte for transaction.
				dataOffset += 8 + FIELD_HEADER_SIZE;

				if (ver.HasValue)
				{
					dataOffset += 7 + FIELD_HEADER_SIZE;
				}

				if (hasWrite && txn.Deadline != 0)
				{
					dataOffset += 4 + FIELD_HEADER_SIZE;
				}
			}
		}

		private void WriteBatchHeader(Policy policy, int timeout, int fieldCount)
		{
			int readAttr = Command.INFO1_BATCH;

			if (policy.compress)
			{
				readAttr |= Command.INFO1_COMPRESS_RESPONSE;
			}

			// Write all header data except total size which must be written last.
			dataOffset += 8;
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)readAttr;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)0;

			Array.Clear(dataBuffer, dataOffset, 10);
			dataOffset += 10;

			dataOffset += ByteUtil.IntToBytes((uint)timeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes(0, dataBuffer, dataOffset);
		}

		private void WriteBatchBinNames(Key key, Txn txn, long? ver, string[] binNames, BatchAttr attr, Expression filter)
		{
			WriteBatchRead(key, txn, ver, attr, filter, binNames.Length);

			foreach (string binName in binNames)
			{
				WriteOperation(binName, Operation.Type.READ);
			}
		}

		private void WriteBatchOperations(Key key, Txn txn, long? ver, Operation[] ops, BatchAttr attr, Expression filter)
		{
			if (attr.hasWrite)
			{
				WriteBatchWrite(key, txn, ver, attr, filter, 0, ops.Length);
			}
			else
			{
				WriteBatchRead(key, txn, ver, attr, filter, ops.Length);
			}

			foreach (Operation op in ops)
			{
				WriteOperation(op);
			}
		}

		private void WriteBatchRead(Key key, Txn txn, long? ver, BatchAttr attr, Expression filter, int opCount)
		{
			if (txn != null)
			{
				dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_TTL);
				dataBuffer[dataOffset++] = (byte)attr.readAttr;
				dataBuffer[dataOffset++] = (byte)attr.writeAttr;
				dataBuffer[dataOffset++] = (byte)attr.infoAttr;
				dataBuffer[dataOffset++] = (byte)attr.txnAttr;
				ByteUtil.IntToBytes((uint)attr.expiration, dataBuffer, dataOffset);
				dataOffset += 4;
				WriteBatchFieldsTxn(key, txn, ver, attr, filter, 0, opCount);
			}
			else
			{
				dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_TTL);
				dataBuffer[dataOffset++] = (byte)attr.readAttr;
				dataBuffer[dataOffset++] = (byte)attr.writeAttr;
				dataBuffer[dataOffset++] = (byte)attr.infoAttr;
				ByteUtil.IntToBytes((uint)attr.expiration, dataBuffer, dataOffset);
				dataOffset += 4;
				WriteBatchFieldsReg(key, attr, filter, 0, opCount);
			}
		}

		private void WriteBatchWrite(Key key, Txn txn, long? ver, BatchAttr attr, Expression filter, int fieldCount, int opCount)
		{
			if (txn != null)
			{
				dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_GEN | BATCH_MSG_TTL);
				dataBuffer[dataOffset++] = (byte)attr.readAttr;
				dataBuffer[dataOffset++] = (byte)attr.writeAttr;
				dataBuffer[dataOffset++] = (byte)attr.infoAttr;
				dataBuffer[dataOffset++] = (byte)attr.txnAttr;
				ByteUtil.ShortToBytes((ushort)attr.generation, dataBuffer, dataOffset);
				dataOffset += 2;
				ByteUtil.IntToBytes((uint)attr.expiration, dataBuffer, dataOffset);
				dataOffset += 4;
				WriteBatchFieldsTxn(key, txn, ver, attr, filter, fieldCount, opCount);
			}
			else
			{
				dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_GEN | BATCH_MSG_TTL);
				dataBuffer[dataOffset++] = (byte)attr.readAttr;
				dataBuffer[dataOffset++] = (byte)attr.writeAttr;
				dataBuffer[dataOffset++] = (byte)attr.infoAttr;
				ByteUtil.ShortToBytes((ushort)attr.generation, dataBuffer, dataOffset);
				dataOffset += 2;
				ByteUtil.IntToBytes((uint)attr.expiration, dataBuffer, dataOffset);
				dataOffset += 4;
				WriteBatchFieldsReg(key, attr, filter, fieldCount, opCount);
			}
		}

		private void WriteBatchFieldsTxn(Key key, Txn txn, long? ver, BatchAttr attr, Expression filter, int fieldCount, int opCount)
		{
			fieldCount++;

			if (ver.HasValue)
			{
				fieldCount++;
			}

			if (attr.hasWrite && txn.Deadline != 0)
			{
				fieldCount++;
			}

			if (filter != null)
			{
				fieldCount++;
			}

			if (attr.sendKey)
			{
				fieldCount++;
			}

			WriteBatchFields(key, fieldCount, opCount);

			WriteFieldLE(txn.Id, FieldType.TXN_ID);

			if (ver.HasValue)
			{
				WriteFieldVersion(ver.Value);
			}

			if (attr.hasWrite && txn.Deadline != 0)
			{
				WriteFieldLE(txn.Deadline, FieldType.TXN_DEADLINE);
			}

			filter?.Write(this);

			if (attr.sendKey)
			{
				WriteField(key.userKey, FieldType.KEY);
			}
		}

		private void WriteBatchFieldsReg(
			Key key,
			BatchAttr attr,
			Expression filter,
			int fieldCount,
			int opCount
		)
		{
			if (filter != null)
			{
				fieldCount++;
			}

			if (attr.sendKey)
			{
				fieldCount++;
			}

			WriteBatchFields(key, fieldCount, opCount);

			filter?.Write(this);

			if (attr.sendKey)
			{
				WriteField(key.userKey, FieldType.KEY);
			}
		}

		private void WriteBatchFields(Key key, int fieldCount, int opCount)
		{
			fieldCount += 2;
			ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += 2;
			ByteUtil.ShortToBytes((ushort)opCount, dataBuffer, dataOffset);
			dataOffset += 2;
			WriteField(key.ns, FieldType.NAMESPACE);
			WriteField(key.setName, FieldType.TABLE);
		}

		//--------------------------------------------------
		// Scan
		//--------------------------------------------------

		public virtual void SetScan
		(
			Cluster cluster,
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
					EstimateOperationSize(binName);
				}
			}

			SizeBuffer();
			int readAttr = Command.INFO1_READ;

			if (!policy.includeBinData)
			{
				readAttr |= Command.INFO1_NOBINDATA;
			}

			// Clusters that support partition queries also support not sending partition done messages.
			int operationCount = (binNames == null) ? 0 : binNames.Length;
			WriteHeaderRead(policy, totalTimeout, readAttr, 0, Command.INFO3_PARTITION_DONE, fieldCount, operationCount);

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

				foreach (PartitionStatus part in nodePartitions.partsPartial)
				{
					Array.Copy(part.digest, 0, dataBuffer, dataOffset, 20);
					dataOffset += 20;
				}
			}

			if (maxRecords > 0)
			{
				WriteField((ulong)maxRecords, FieldType.MAX_RECORDS);
			}

			if (policy.recordsPerSecond > 0)
			{
				WriteField(policy.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			policy.filterExp?.Write(this);

			// Write scan timeout
			WriteField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			WriteField(taskId, FieldType.QUERY_ID);

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		//--------------------------------------------------
		// Query
		//--------------------------------------------------

		protected virtual internal void SetQuery
		(
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

			Begin();

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

				if (statement.filter.IndexName != null)
				{
					dataOffset += ByteUtil.EstimateSizeUtf8(statement.filter.IndexName) + 1;
					fieldCount++;
				}

				if (statement.filter.Exp != null)
				{
					dataOffset += statement.Filter.Exp.Size();
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
					EstimateOperationSize(operation);
				}
				operationCount = statement.operations.Length;
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				// Estimate size for selected bin names (query bin names already handled for old servers).
				foreach (string binName in statement.binNames)
				{
					EstimateOperationSize(binName);
				}
				operationCount = statement.binNames.Length;
			}

			SizeBuffer();

			if (background)
			{
				WriteHeaderWrite((WritePolicy)policy, Command.INFO2_WRITE, fieldCount, operationCount);
			}
			else
			{
				QueryPolicy qp = (QueryPolicy)policy;
				int readAttr = Command.INFO1_READ;
				int writeAttr = 0;

				if (!qp.includeBinData)
				{
					readAttr |= Command.INFO1_NOBINDATA;
				}

				if (qp.shortQuery || qp.expectedDuration == QueryDuration.SHORT)
				{
					readAttr |= Command.INFO1_SHORT_QUERY;
				}
				else if (qp.expectedDuration == QueryDuration.LONG_RELAX_AP)
				{
					writeAttr |= Command.INFO2_RELAX_AP_LONG_QUERY;
				}

				int infoAttr = (isNew || statement.filter == null) ? Command.INFO3_PARTITION_DONE : 0;

				WriteHeaderRead(policy, totalTimeout, readAttr, writeAttr, infoAttr, fieldCount, operationCount);
			}

			if (statement.ns != null)
			{
				WriteField(statement.ns, FieldType.NAMESPACE);
			}

			if (statement.setName != null)
			{
				WriteField(statement.setName, FieldType.TABLE);
			}

			// Write records per second.
			if (statement.recordsPerSecond > 0)
			{
				WriteField(statement.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			// Write socket idle timeout.
			WriteField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			WriteField(taskId, FieldType.QUERY_ID);

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

				if (!isNew)
				{
					// Query bin names are specified as a field (Scan bin names are specified later as operations)
					// in old servers.
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

				if (packedCtx != null)
				{
					WriteFieldHeader(packedCtx.Length, FieldType.INDEX_CONTEXT);
					Array.Copy(packedCtx, 0, dataBuffer, dataOffset, packedCtx.Length);
					dataOffset += packedCtx.Length;
				}

				if (statement.filter.IndexName != null)
				{
					WriteField(statement.filter.IndexName, FieldType.INDEX_NAME);
				}

				statement.filter.Exp?.WriteIndex(this);
			}

			if (statement.functionName != null)
			{
				WriteFieldHeader(1, FieldType.UDF_OP);
				dataBuffer[dataOffset++] = background ? (byte)2 : (byte)1;
				WriteField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
				WriteField(statement.functionName, FieldType.UDF_FUNCTION);
				WriteField(functionArgBuffer, FieldType.UDF_ARGLIST);
			}

			policy.filterExp?.Write(this);

			if (partsFullSize > 0)
			{
				WriteFieldHeader(partsFullSize, FieldType.PID_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsFull)
				{
					ByteUtil.ShortToLittleBytes((ushort)part.id, dataBuffer, dataOffset);
					dataOffset += 2;
				}
			}

			if (partsPartialDigestSize > 0)
			{
				WriteFieldHeader(partsPartialDigestSize, FieldType.DIGEST_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial)
				{
					Array.Copy(part.digest, 0, dataBuffer, dataOffset, 20);
					dataOffset += 20;
				}
			}

			if (partsPartialBValSize > 0)
			{
				WriteFieldHeader(partsPartialBValSize, FieldType.BVAL_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial)
				{
					ByteUtil.LongToLittleBytes(part.bval, dataBuffer, dataOffset);
					dataOffset += 8;
				}
			}

			if (maxRecords > 0)
			{
				WriteField((ulong)maxRecords, FieldType.MAX_RECORDS);
			}

			if (statement.operations != null)
			{
				foreach (Operation operation in statement.operations)
				{
					WriteOperation(operation);
				}
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				foreach (string binName in statement.binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		//--------------------------------------------------
		// Command Sizing
		//--------------------------------------------------

		private int EstimateKeyAttrSize(Policy policy, Key key, BatchAttr attr, Expression filterExp)
		{
			int fieldCount = EstimateKeySize(policy, key, attr.hasWrite);

			if (filterExp != null)
			{
				dataOffset += filterExp.Size();
				fieldCount++;
			}
			return fieldCount;
		}

		private int EstimateKeySize(Policy policy, Key key, bool hasWrite)
		{
			int fieldCount = EstimateKeySize(key);

			fieldCount += SizeTxn(key, policy.Txn, hasWrite);

			if (policy.sendKey)
			{
				dataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}
			return fieldCount;
		}

		private int EstimateKeySize(Key key)
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

		private void EstimateReadOperationSize(Operation operation)
		{
			if (Operation.IsWrite(operation.type))
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
			}
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

		//--------------------------------------------------
		// Command Writes
		//--------------------------------------------------

		/// <summary>
		/// Header write for write commands.
		/// </summary>
		private void WriteHeaderWrite(WritePolicy policy, int writeAttr, int fieldCount, int operationCount)
		{
			// Set flags.
			int generation = 0;
			int infoAttr = 0;
			int txnAttr = 0;

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

			if (policy.OnLockingOnly)
			{
				txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
			}

			dataOffset += 8;

			// Write all header data except total size which must be written last. 
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)writeAttr;
			dataBuffer[dataOffset++] = (byte)infoAttr;
			dataBuffer[dataOffset++] = (byte)txnAttr;
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
		private void WriteHeaderReadWrite
		(
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
			int txnAttr = 0;
			int operationCount = args.operations.Length;

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

			if (policy.OnLockingOnly)
			{
				txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
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
			dataBuffer[dataOffset++] = (byte)txnAttr;
			dataBuffer[dataOffset++] = 0; // clear the result code
			dataOffset += ByteUtil.IntToBytes((uint)generation, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)ttl, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for read commands.
		/// </summary>
		private void WriteHeaderRead
		(
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

			for (int i = 0; i < 6; i++)
			{
				dataBuffer[dataOffset++] = 0;
			}
			dataOffset += ByteUtil.IntToBytes((uint)policy.readTouchTtlPercent, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);
		}

		/// <summary>
		/// Header write for batch single commands.
		/// </summary>
		private void WriteKeyAttr(
			Policy policy,
			Key key,
			BatchAttr attr,
			Expression filterExp,
			int fieldCount,
			int operationCount
		)
		{
			dataOffset += 8;

			// Write all header data except total size which must be written last.
			dataBuffer[dataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			dataBuffer[dataOffset++] = (byte)attr.txnAttr;
			dataBuffer[dataOffset++] = 0; // clear the result code
			dataOffset += ByteUtil.IntToBytes((uint)attr.generation, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)attr.expiration, dataBuffer, dataOffset);
			dataOffset += ByteUtil.IntToBytes((uint)serverTimeout, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, dataOffset);
			dataOffset += ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, dataOffset);

			WriteKey(policy, key, attr.hasWrite);

			filterExp?.Write(this);
		}

		private void WriteKey(Policy policy, Key key, bool sendDeadline)
		{
			WriteKey(key);
			WriteTxn(policy.Txn, sendDeadline);

			if (policy.sendKey)
			{
				WriteField(key.userKey, FieldType.KEY);
			}
		}

		private void WriteKey(Key key)
		{
			// Write key into dataBuffer.
			if (key.ns != null)
			{
				WriteField(key.ns, FieldType.NAMESPACE);
			}

			if (key.setName != null)
			{
				WriteField(key.setName, FieldType.TABLE);
			}

			WriteField(key.digest, FieldType.DIGEST_RIPE);
		}

		private int WriteReadOnlyOperations(Operation[] ops, int readAttr)
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
							readAttr |= Command.INFO1_GET_ALL;
						}
						readBin = true;
						break;

					case Operation.Type.READ_HEADER:
						readHeader = true;
						break;

					default:
						break;
				}
				WriteOperation(op);
			}

			if (readHeader && !readBin)
			{
				readAttr |= Command.INFO1_NOBINDATA;
			}
			return readAttr;
		}

		private void WriteOperation(Bin bin, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(bin.name, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);
			int valueLength = bin.value.Write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operationType);
			dataBuffer[dataOffset++] = (byte)bin.value.Type;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)nameLength;
			dataOffset += nameLength + valueLength;
		}

		private void WriteOperation(Operation operation)
		{
			int nameLength = ByteUtil.StringToUtf8(operation.binName, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);
			int valueLength = operation.value.Write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operation.type);
			dataBuffer[dataOffset++] = (byte)operation.value.Type;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)nameLength;
			dataOffset += nameLength + valueLength;
		}

		private void WriteOperation(string name, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(name, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);

			ByteUtil.IntToBytes((uint)(nameLength + 4), dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = Operation.GetProtocolType(operationType);
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)0;
			dataBuffer[dataOffset++] = (byte)nameLength;
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

		private int SizeTxn(Key key, Txn txn, bool hasWrite)
		{
			int fieldCount = 0;

			if (txn != null)
			{
				dataOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;

				Version = txn.GetReadVersion(key);

				if (Version.HasValue)
				{
					dataOffset += 7 + FIELD_HEADER_SIZE;
					fieldCount++;
				}

				if (hasWrite && txn.Deadline != 0)
				{
					dataOffset += 4 + FIELD_HEADER_SIZE;
					fieldCount++;
				}
			}
			return fieldCount;
		}

		private void WriteTxn(Txn txn, bool sendDeadline)
		{
			if (txn != null)
			{
				WriteFieldLE(txn.Id, FieldType.TXN_ID);

				if (Version.HasValue)
				{
					WriteFieldVersion(Version.Value);
				}

				if (sendDeadline && txn.Deadline != 0)
				{
					WriteFieldLE(txn.Deadline, FieldType.TXN_DEADLINE);
				}
			}
		}

		private void WriteFieldVersion(long ver)
		{
			WriteFieldHeader(7, FieldType.RECORD_VERSION);
			ByteUtil.LongToVersionBytes(ver, dataBuffer, dataOffset);
			dataOffset += 7;
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
			ByteUtil.IntToBytes((uint)val, dataBuffer, dataOffset);
			dataOffset += 4;
		}

		private void WriteFieldLE(int val, int type)
		{
			WriteFieldHeader(4, type);
			ByteUtil.IntToLittleBytes((uint)val, dataBuffer, dataOffset);
			dataOffset += 4;
		}

		private void WriteField(ulong val, int type)
		{
			WriteFieldHeader(8, type);
			ByteUtil.LongToBytes(val, dataBuffer, dataOffset);
			dataOffset += 8;
		}

		private void WriteFieldLE(long val, int type)
		{
			WriteFieldHeader(8, type);
			ByteUtil.LongToLittleBytes((ulong)val, dataBuffer, dataOffset);
			dataOffset += 8;
		}

		private void WriteFieldHeader(int size, int type)
		{
			ByteUtil.IntToBytes((uint)size + 1, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = (byte)type;
		}

		internal void WriteExpHeader(int size, int type)
		{
			WriteFieldHeader(size, type);
		}

		private void Begin()
		{
			dataOffset = MSG_TOTAL_HEADER_SIZE;
		}

		private bool SizeBuffer(Policy policy)
		{
			if (policy.compress && dataOffset > COMPRESS_THRESHOLD)
			{
				// Command will be compressed. First, write uncompressed command
				// into separate dataBuffer. Save normal dataBuffer for compressed command.
				// Normal dataBuffer in async mode is from dataBuffer pool that is used to
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

			// Increase requested dataBuffer size in case compressed dataBuffer size is
			// greater than the uncompressed dataBuffer size.
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

		//--------------------------------------------------
		// Response Parsing
		//--------------------------------------------------

		internal virtual void SkipKey(int fieldCount)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldlen;
			}
		}

		internal virtual Key ParseKey(int fieldCount, out ulong bval)
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

		public long? ParseVersion(int fieldCount)
		{
			long? version = null;

			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int type = dataBuffer[dataOffset++];
				int size = len - 1;

				if (type == FieldType.RECORD_VERSION && size == 7)
				{
					version = ByteUtil.VersionBytesToLong(dataBuffer, dataOffset);
				}
				dataOffset += size;
			}
			return version;
		}

		protected void ParseFields(Txn txn, Key key, bool hasWrite)
		{
			if (txn == null)
			{
				SkipFields(fieldCount);
				return;
			}

			long? version = null;

			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int type = dataBuffer[dataOffset++];
				int size = len - 1;

				if (type == FieldType.RECORD_VERSION)
				{
					if (size == 7)
					{
						version = ByteUtil.VersionBytesToLong(dataBuffer, dataOffset);
					}
					else
					{
						throw new AerospikeException("Record version field has invalid size: " + size);
					}
				}
				dataOffset += size;
			}

			if (hasWrite)
			{
				txn.OnWrite(key, version, resultCode);
			}
			else
			{
				txn.OnRead(key, version);
			}
		}

		protected void ParseTxnDeadline(Txn txn)
		{
			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int type = dataBuffer[dataOffset++];
				int size = len - 1;

				if (type == FieldType.TXN_DEADLINE)
				{
					int deadline = ByteUtil.LittleBytesToInt(dataBuffer, dataOffset);
					txn.Deadline = deadline;
				}
				dataOffset += size;
			}
		}

		protected void SkipFields(int fieldCount)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldlen;
			}
		}

		public static bool BatchInDoubt(bool isWrite, int commandSentCounter)
		{
			return isWrite && commandSentCounter > 1;
		}

		public interface BatchOffsets
		{
			int Size();
			int Get(int i);
		}

		private class BatchOffsetsNative : BatchOffsets
		{
			private int size;
			private int[] offsets;

			public BatchOffsetsNative(BatchNode batch)
			{
				this.size = batch.offsetsSize;
				this.offsets = batch.offsets;
			}

			public int Size()
			{
				return size;
			}

			public int Get(int i)
			{
				return offsets[i];
			}
		}
	}
}
#pragma warning restore 0618
