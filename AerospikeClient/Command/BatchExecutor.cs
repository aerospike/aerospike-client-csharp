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
using System.Threading;

namespace Aerospike.Client
{
	public sealed class BatchExecutor
	{
		public static void Execute(Cluster cluster, BatchPolicy policy, Key[] keys, bool[] existsArray, Record[] records, HashSet<string> binNames, int readAttr)
		{
			if (keys.Length == 0)
			{
				return;
			}

			if (policy.allowProleReads)
			{
				// Send all requests to a single node chosen in round-robin fashion in this transaction thread.
				Node node = cluster.GetRandomNode();

				if (records != null)
				{
					BatchCommandNodeGet command = new BatchCommandNodeGet(node, policy, keys, records, binNames, readAttr);
					command.Execute();
				}
				else
				{
					BatchCommandNodeExists command = new BatchCommandNodeExists(node, policy, keys, existsArray);
					command.Execute();
				}
				return;
			}

			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys);

			if (policy.maxConcurrentThreads == 1)
			{
				// Run batch requests sequentially in same thread.
				foreach (BatchNode batchNode in batchNodes)
				{
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						if (records != null)
						{
							BatchCommandGet command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
							command.Execute();
						}
						else
						{
							BatchCommandExists command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray);
							command.Execute();
						}
					}
				}
			}
			else
			{
				// Run batch requests in parallel in separate threads.
				Executor executor = new Executor(batchNodes.Count * 2);

				// Initialize threads.  There may be multiple threads for a single node because the
				// wire protocol only allows one namespace per command.  Multiple namespaces 
				// require multiple threads per node.
				foreach (BatchNode batchNode in batchNodes)
				{
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						if (records != null)
						{
							MultiCommand command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
							executor.AddCommand(command);
						}
						else
						{
							MultiCommand command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray);
							executor.AddCommand(command);
						}
					}
				}

				executor.Execute(policy.maxConcurrentThreads);
			}
		}
	}
}
