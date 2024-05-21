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

namespace Aerospike.Client
{
	public class AsyncRead : AsyncSingleCommand
	{
		private readonly RecordListener listener;
		protected internal readonly Key key;
		private readonly string[] binNames;
		private readonly bool isOperation;
		protected readonly Partition partition;
		protected Record record;

		// Read constructor.
		public AsyncRead(AsyncCluster cluster, Policy policy, RecordListener listener, Key key, string[] binNames) 
			: base(cluster, policy)
		{
			this.listener = listener;
			this.key = key;
			this.binNames = binNames;
			this.isOperation = false;
			this.partition = Partition.Read(cluster, policy, key);
			cluster.AddTran();
		}

		// UDF constructor.
		public AsyncRead(AsyncCluster cluster, WritePolicy policy, Key key)
			: base(cluster, policy)
		{
			this.listener = null;
			this.key = key;
			this.binNames = null;
			this.isOperation = false;
			this.partition = Partition.Write(cluster, policy, key);
			cluster.AddTran();
		}

		// Operate constructor.
		public AsyncRead(AsyncCluster cluster, Policy policy, RecordListener listener, Key key, Partition partition, bool isOperation)
			: base(cluster, policy)
		{
			this.listener = listener;
			this.key = key;
			this.binNames = null;
			this.isOperation = isOperation;
			this.partition = partition;
			cluster.AddTran();
		}

		public AsyncRead(AsyncRead other)
			: base(other)
		{
			this.listener = other.listener;
			this.key = other.key;
			this.binNames = other.binNames;
			this.isOperation = other.isOperation;
			this.partition = other.partition;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncRead(this);
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return partition.GetNodeRead(cluster);
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.READ;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal sealed override void ParseResult()
		{
			int resultCode = dataBuffer[dataOffset + 5];
			int generation = ByteUtil.BytesToInt(dataBuffer, dataOffset + 6);
			int expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset + 10);
			int fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 18);
			int opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 20);
			dataOffset += Command.MSG_REMAINING_HEADER_SIZE;

			if (resultCode == 0)
			{
				if (opCount == 0)
				{
					// Bin data was not returned.
					record = new Record(null, generation, expiration);
					return;
				}
				SkipKey(fieldCount);
				(record, dataOffset) = policy.recordParser.ParseRecord(dataBuffer, dataOffset, opCount, generation, expiration, isOperation);
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
				SkipKey(fieldCount);
				(record, dataOffset) = policy.recordParser.ParseRecord(dataBuffer, dataOffset, opCount, generation, expiration, isOperation);
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

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key, record);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}
