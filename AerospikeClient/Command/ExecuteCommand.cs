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

using Aerospike.Client;

namespace Aerospike.Client
{
	public sealed class ExecuteCommand : SyncWriteCommand
	{
		private readonly string packageName;
		private readonly string functionName;
		private readonly Value[] args;
		public Record Record {  get; private set; }

		public ExecuteCommand
		(
			Cluster cluster,
			WritePolicy writePolicy,
			Key key,
			string packageName,
			string functionName,
			Value[] args
		) : base(cluster, writePolicy, key)
		{
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(writePolicy, key, packageName, functionName, args);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			ParseHeader(conn);
			ParseFields(policy.Txn, key, true);

			if (resultCode == ResultCode.OK)
			{
				Record = policy.recordParser.ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, false);
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				Record = policy.recordParser.ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, false);
				HandleUdfError(resultCode);
				return;
			}

			if (opCount > 0)
			{
				throw new AerospikeException("Unexpected UDF opCount on error: " + opCount + ',' + resultCode);
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}

		private void HandleUdfError(int resultCode)
		{
			string ret = (string)Record.bins["FAILURE"];

			if (ret == null)
			{
				throw new AerospikeException(resultCode);
			}

			string message;
			int code;

			try
			{
				string[] list = ret.Split(":");
				Int32.TryParse(list[2].Trim(), out code);
				message = list[0] + ':' + list[1] + ' ' + list[3];
			}
			catch (Exception e)
			{
				// Use generic exception if parse error occurs.
				throw new AerospikeException(resultCode, ret);
			}

			throw new AerospikeException(code, message);
		}
	}
}
