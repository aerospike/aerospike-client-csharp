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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncOperate : AsyncRead
	{
		private readonly WritePolicy writePolicy;
		private readonly Operation[] operations;

		public AsyncOperate(AsyncCluster cluster, WritePolicy policy, RecordListener listener, Key key, Operation[] operations) 
			: base(cluster, policy, listener, key, null)
		{
			this.writePolicy = policy;
			this.operations = operations;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(writePolicy, key, operations);
		}

		protected internal override AsyncNode GetNode()
		{
			return (AsyncNode)cluster.GetMasterNode(partition);
		}
	}
}
