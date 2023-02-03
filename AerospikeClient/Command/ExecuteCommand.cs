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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ExecuteCommand : ReadCommand
	{
		private readonly WritePolicy writePolicy;
		private readonly string packageName;
		private readonly string functionName;
		private readonly Value[] args;

		public ExecuteCommand
		(
			Cluster cluster,
			WritePolicy writePolicy,
			Key key,
			string packageName,
			string functionName,
			Value[] args
		) : base(cluster, writePolicy, key, Partition.Write(cluster, writePolicy, key), false)
		{
			this.writePolicy = writePolicy;
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode()
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
	}
}
