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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncExecute : AsyncRead
	{
		private readonly WritePolicy writePolicy;
		private readonly ExecuteListener executeListener;
		private readonly string packageName;
		private readonly string functionName;
		private readonly Value[] args;

		public AsyncExecute
		(
			AsyncCluster cluster,
			WritePolicy writePolicy,
			ExecuteListener listener,
			Key key,
			string packageName,
			string functionName,
			Value[] args
		) : base(cluster, writePolicy, key)
		{
			this.writePolicy = writePolicy;
			this.executeListener = listener;
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
		}

		public AsyncExecute(AsyncExecute other)
			: base(other)
		{
			this.writePolicy = other.writePolicy;
			this.executeListener = other.executeListener;
			this.packageName = other.packageName;
			this.functionName = other.functionName;
			this.args = other.args;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncExecute(this);
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return partition.GetNodeWrite(cluster);
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(writePolicy, key, packageName, functionName, args);
		}

		protected internal override void HandleNotFound(int resultCode)
		{
			throw new AerospikeException(resultCode);
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryWrite(timeout);
			return true;
		}

		protected internal override void OnSuccess()
		{
			if (executeListener != null)
			{
				object obj = ParseEndResult();
				executeListener.OnSuccess(key, obj);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (executeListener != null)
			{
				executeListener.OnFailure(e);
			}
		}

		private object ParseEndResult()
		{
			if (record == null || record.bins == null)
			{
				return null;
			}

			IDictionary<string, object> map = record.bins;

			object obj = map["SUCCESS"];

			if (obj != null)
			{
				return obj;
			}

			// User defined functions don't have to return a value.
			if (map.ContainsKey("SUCCESS"))
			{
				return null;
			}

			obj = map["FAILURE"];

			if (obj != null)
			{
				throw new AerospikeException(obj.ToString());
			}
			throw new AerospikeException("Invalid UDF return value");
		}
	}
}
