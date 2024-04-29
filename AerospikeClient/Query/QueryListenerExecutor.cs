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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class QueryListenerExecutor
	{
		public static void execute
		(
			Cluster cluster,
			QueryPolicy policy,
			Statement statement,
			QueryListener listener,
			PartitionTracker tracker
		)
		{
			cluster.AddTran();
			
			ulong taskId = statement.PrepareTaskId();

			while (true)
			{
				try
				{
					List<NodePartitions> list = tracker.AssignPartitionsToNodes(cluster, statement.ns);

					if (policy.maxConcurrentNodes > 0 && list.Count > 1)
					{
						Executor executor = new Executor(list.Count);

						foreach (NodePartitions nodePartitions in list)
						{
							QueryListenerCommand command = new QueryListenerCommand(cluster, policy, statement, taskId, listener, tracker, nodePartitions);
							executor.AddCommand(command);
						}

						executor.Execute(policy.maxConcurrentNodes);
					}
					else
					{
						foreach (NodePartitions nodePartitions in list)
						{
							QueryListenerCommand command = new QueryListenerCommand(cluster, policy, statement, taskId, listener, tracker, nodePartitions);
							command.Execute();
						}
					}
				}
				catch (AerospikeException ae)
				{
					tracker.PartitionError();
					ae.Iteration = tracker.iteration;
					throw ae;
				}

				if (tracker.IsComplete(cluster, policy))
				{
					return;
				}

				if (policy.sleepBetweenRetries > 0)
				{
					// Sleep before trying again.
					Util.Sleep(policy.sleepBetweenRetries);
				}

				// taskId must be reset on next pass to avoid server duplicate query detection.
				taskId = RandomShift.ThreadLocalInstance.NextLong();
			}
		}
	}
}
