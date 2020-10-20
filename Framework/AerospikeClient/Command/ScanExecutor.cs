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
using System;
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class ScanExecutor
	{
		public static void ScanPartitions(Cluster cluster, ScanPolicy policy, string ns, string setName, string[] binNames, ScanCallback callback, PartitionTracker tracker)
		{
			policy.Validate();

			while (true)
			{
				ulong taskId = RandomShift.ThreadLocalInstance.NextLong();

				try
				{
					List<NodePartitions> list = tracker.AssignPartitionsToNodes(cluster, ns);

					if (policy.concurrentNodes && list.Count > 1)
					{
						Executor executor = new Executor(list.Count);

						foreach (NodePartitions nodePartitions in list)
						{
							ScanPartitionCommand command = new ScanPartitionCommand(cluster, policy, ns, setName, binNames, callback, taskId, tracker, nodePartitions);
							executor.AddCommand(command);
						}

						executor.Execute(policy.maxConcurrentNodes);
					}
					else
					{
						foreach (NodePartitions nodePartitions in list)
						{
							ScanPartitionCommand command = new ScanPartitionCommand(cluster, policy, ns, setName, binNames, callback, taskId, tracker, nodePartitions);
							command.Execute();
						}
					}
				}
				catch (AerospikeException ae)
				{
					ae.Iteration = tracker.iteration;
					throw ae;
				}

				if (tracker.IsComplete(policy))
				{
					// Scan is complete.
					return;
				}

				if (policy.sleepBetweenRetries > 0)
				{
					// Sleep before trying again.
					Util.Sleep(policy.sleepBetweenRetries);
				}
			}
		}
	}
}
