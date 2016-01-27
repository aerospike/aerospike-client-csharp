/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
	public sealed class AsyncTouch : AsyncSingleCommand
	{
		private readonly WritePolicy policy;
		private readonly WriteListener listener;
		private readonly Key key;
		private readonly Partition partition;

		public AsyncTouch(AsyncCluster cluster, WritePolicy policy, WriteListener listener, Key key) 
			: base(cluster)
		{
			this.policy = policy;
			this.listener = listener;
			this.key = key;
			this.partition = new Partition(key);
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetTouch(policy, key);
		}

		protected internal override AsyncNode GetNode()
		{
			return (AsyncNode)cluster.GetMasterNode(partition);
		}

		protected internal override void ParseResult()
		{
			int resultCode = dataBuffer[dataOffset + 5];

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key);
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
