/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
	public abstract class AsyncBatchExecutor : AsyncMultiExecutor
	{
		protected internal readonly Key[] keys;
		protected internal readonly List<BatchNode> batchNodes;

		public AsyncBatchExecutor(Cluster cluster, Key[] keys)
		{
			this.keys = keys;
			this.batchNodes = BatchNode.GenerateList(cluster, keys);

			// Count number of asynchronous commands needed.
			int size = 0;
			foreach (BatchNode batchNode in batchNodes)
			{
				size += batchNode.batchNamespaces.Count;
			}
			completedSize = size;
		}
	}
}
