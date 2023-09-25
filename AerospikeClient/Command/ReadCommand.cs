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
using Aerospike.Client.KVS;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace Aerospike.Client
{
	public class ReadCommand : SyncCommand
	{
		protected readonly Key key;
		protected readonly Partition partition;
		private readonly string[] binNames;
		private readonly bool isOperation;
		private Record record;

		public ReadCommand(Cluster cluster, Policy policy, Key key)
			: base(cluster, policy)
		{
			this.key = key;
			this.binNames = null;
			this.partition = Partition.Read(cluster, policy, key);
			this.isOperation = false;
		}

		public ReadCommand(Cluster cluster, Policy policy, Key key, String[] binNames)
			: base(cluster, policy)
		{
			this.key = key;
			this.binNames = binNames;
			this.partition = Partition.Read(cluster, policy, key);
			this.isOperation = false;
		}

		public ReadCommand(Cluster cluster, Policy policy, Key key, Partition partition, bool isOperation)
			: base(cluster, policy)
		{
			this.key = key;
			this.binNames = null;
			this.partition = partition;
			this.isOperation = isOperation;
		}

		protected internal override Node GetNode()
		{
			return partition.GetNodeRead(cluster);
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, 8);

			long sz = ByteUtil.BytesToLong(dataBuffer, 0);
			int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

			if (receiveSize <= 0)
			{
				throw new AerospikeException("Invalid receive size: " + receiveSize);
			}

			SizeBuffer(receiveSize);
			conn.ReadFully(dataBuffer, receiveSize);
			conn.UpdateLastUsed();

			ulong type = (ulong)((sz >> 48) & 0xff);

			if (type == Command.AS_MSG_TYPE)
			{
				dataOffset = 5;
			}
			else if (type == Command.MSG_TYPE_COMPRESSED)
			{
				int usize = (int)ByteUtil.BytesToLong(dataBuffer, 0);
				byte[] ubuf = new byte[usize];

				ByteUtil.Decompress(dataBuffer, 8, receiveSize, ubuf, usize);
				dataBuffer = ubuf;
				dataOffset = 13;
			}
			else
			{
				throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
			}
					
			int resultCode = dataBuffer[dataOffset];
			dataOffset++;
			int generation = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			int expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 8;
			int fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			int opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;

			if (resultCode == 0)
			{
				if (opCount == 0)
				{
					// Bin data was not returned.
					record = new Record(null, generation, expiration);
					return;
				}
				SkipKey(fieldCount, dataBuffer);
				record = policy.recordParser.ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, isOperation);
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				HandleNotFound(resultCode);
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				SkipKey(fieldCount, dataBuffer);
				record = policy.recordParser.ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, isOperation);
				HandleUdfError(resultCode);
				return;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryRead(timeout);
			return true;
		}

		protected internal virtual void HandleNotFound(int resultCode)
		{
			// Do nothing in default case. Record will be null.
		}

		private void HandleUdfError(int resultCode)
		{
			object obj;

			if (!record.bins.TryGetValue("FAILURE", out obj))
			{
				throw new AerospikeException(resultCode);
			}

			string ret = (string)obj;
			string message;
			int code;

			try
			{
				string[] list = ret.Split(':');
				code = Convert.ToInt32(list[2].Trim());
				message = list[0] + ':' + list[1] + ' ' + list[3];
			}
			catch (Exception)
			{
				// Use generic exception if parse error occurs.
				throw new AerospikeException(resultCode, ret);
			}

			throw new AerospikeException(code, message);
		}

		public Record Record
		{
			get
			{
				return record;
			}
		}

		public virtual void ExecuteGRPC(CallInvoker callInvoker)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};
			GRPCConversions.SetRequestPolicy(policy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(callInvoker);
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = client.Read(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}

		public virtual async Task<Record> ExecuteGRPC(CallInvoker callInvoker, CancellationToken token)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};
			GRPCConversions.SetRequestPolicy(policy, request);

			var client = new KVS.KVS.KVSClient(callInvoker);
			deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
			var response = await client.ReadAsync(request, deadline: deadline, cancellationToken: token);
			var conn = new ConnectionProxy(response);
			ParseResult(conn);
			return record;
		}
	}
}
