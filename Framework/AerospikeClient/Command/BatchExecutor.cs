/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
		public static void Execute
		(
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			bool[] existsArray,
			Record[] records,
			string[] binNames,
			int readAttr
		)
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
				ExecuteNode(cluster, batchNode, policy, keys, existsArray, records, binNames, readAttr);
				return;
			}

			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys);

			if (policy.maxConcurrentThreads == 1 || batchNodes.Count <= 1)
			{
				// Run batch requests sequentially in same thread.
				foreach (BatchNode batchNode in batchNodes)
				{
					ExecuteNode(cluster, batchNode, policy, keys, existsArray, records, binNames, readAttr);
				}
			}
			else
			{
				// Run batch requests in parallel in separate threads.
				//
				// Multiple threads write to the record/exists array, so one might think that
				// volatile or memory barriers are needed on the write threads and this read thread.
				// This should not be necessary here because it happens in Executor which does a 
				// volatile write (Interlocked.Increment(ref completedCount)) at the end of write threads
				// and a synchronized WaitTillComplete() in this thread.
				Executor executor = new Executor(cluster, policy, batchNodes.Count * 2);

				// Initialize threads.  
				foreach (BatchNode batchNode in batchNodes)
				{
					if (records != null)
					{
						MultiCommand command = new BatchGetArrayCommand(batchNode, policy, keys, binNames, records, readAttr);
						executor.AddCommand(batchNode.node, command);
					}
					else
					{
						MultiCommand command = new BatchExistsArrayCommand(batchNode, policy, keys, existsArray);
						executor.AddCommand(batchNode.node, command);
					}
				}
				executor.Execute(policy.maxConcurrentThreads);
			}
		}

		private static void ExecuteNode(Cluster cluster, BatchNode batchNode, BatchPolicy policy, Key[] keys, bool[] existsArray, Record[] records, string[] binNames, int readAttr)
		{
			if (records != null)
			{
				MultiCommand command = new BatchGetArrayCommand(batchNode, policy, keys, binNames, records, readAttr);
				command.Execute(cluster, policy, null, batchNode.node, true);
			}
			else
			{
				MultiCommand command = new BatchExistsArrayCommand(batchNode, policy, keys, existsArray);
				command.Execute(cluster, policy, null, batchNode.node, true);
			}
		}
	}
}
