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
		public static void Execute(Cluster cluster, BatchPolicy policy, Key[] keys, bool[] existsArray, Record[] records, string[] binNames, int readAttr)
		{
			if (keys.Length == 0)
			{
				return;
			}

			if (policy.allowProleReads)
			{
				// Send all requests to a single node chosen in round-robin fashion in this transaction thread.
				Node node = cluster.GetRandomNode();
				BatchNode batchNode = new BatchNode(node, keys);
				ExecuteNode(batchNode, policy, keys, existsArray, records, binNames, readAttr);
				return;
			}

			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys);

			if (policy.maxConcurrentThreads == 1 || batchNodes.Count <= 1)
			{
				// Run batch requests sequentially in same thread.
				foreach (BatchNode batchNode in batchNodes)
				{
					ExecuteNode(batchNode, policy, keys, existsArray, records, binNames, readAttr);
				}
			}
			else
			{
				// Run batch requests in parallel in separate threads.
				Executor executor = new Executor(batchNodes.Count * 2);

				// Initialize threads.  
				foreach (BatchNode batchNode in batchNodes)
				{
					if (batchNode.node.UseNewBatch(policy))
					{
						// New batch
						if (records != null)
						{
							MultiCommand command = new BatchGetArrayCommand(batchNode, policy, keys, binNames, records, readAttr);
							executor.AddCommand(command);
						}
						else
						{
							MultiCommand command = new BatchExistsArrayCommand(batchNode, policy, keys, existsArray);
							executor.AddCommand(command);
						}
					}
					else
					{
						// There may be multiple threads for a single node because the
						// wire protocol only allows one namespace per command.  Multiple namespaces 
						// require multiple threads per node.
						batchNode.SplitByNamespace(keys);

						foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
						{
							if (records != null)
							{
								MultiCommand command = new BatchGetArrayDirect(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
								executor.AddCommand(command);
							}
							else
							{
								MultiCommand command = new BatchExistsArrayDirect(batchNode.node, batchNamespace, policy, keys, existsArray);
								executor.AddCommand(command);
							}
						}
					}
				}
				executor.Execute(policy.maxConcurrentThreads);
			}
		}

		private static void ExecuteNode(BatchNode batchNode, BatchPolicy policy, Key[] keys, bool[] existsArray, Record[] records, string[] binNames, int readAttr)
		{
			if (batchNode.node.UseNewBatch(policy))
			{
				// New batch
				if (records != null)
				{
					MultiCommand command = new BatchGetArrayCommand(batchNode, policy, keys, binNames, records, readAttr);
					command.Execute();
				}
				else
				{
					MultiCommand command = new BatchExistsArrayCommand(batchNode, policy, keys, existsArray);
					command.Execute();
				}
			}
			else
			{
				// Old batch only allows one namespace per call.
				batchNode.SplitByNamespace(keys);

				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					if (records != null)
					{
						MultiCommand command = new BatchGetArrayDirect(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
						command.Execute();
					}
					else
					{
						MultiCommand command = new BatchExistsArrayDirect(batchNode.node, batchNamespace, policy, keys, existsArray);
						command.Execute();
					}
				}
			}
		}
	}
}
