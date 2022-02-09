/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	/// <summary>
	/// Task used to poll for long running execute job completion.
	/// </summary>
	public sealed class ExecuteTask : BaseTask
	{
		private readonly ulong taskId;
		private readonly bool scan;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public ExecuteTask(Cluster cluster, Policy policy, Statement statement, ulong taskId)
			: base(cluster, policy)
		{
			this.taskId = taskId;
			this.scan = statement.filter == null;
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override int QueryStatus()
		{
			// All nodes must respond with complete to be considered done.
			Node[] nodes = cluster.ValidateNodes();
			
			string module = (scan) ? "scan" : "query";
			string cmd1 = "query-show:trid=" + taskId;
			string cmd2 = module + "-show:trid=" + taskId;
			string cmd3 = "jobs:module=" + module + ";cmd=get-job;trid=" + taskId;

			foreach (Node node in nodes)
			{
				string command;

				if (node.HasPartitionQuery)
				{
					// query-show works for both scan and query.
					command = cmd1;
				}
				else if (node.HasQueryShow)
				{
					// scan-show and query-show are separate.
					command = cmd2;
				}
				else
				{
					// old job monitor syntax.
					command = cmd3;
				}

				string response = Info.Request(policy, node, command);

				if (response.StartsWith("ERROR:2"))
				{
					return BaseTask.NOT_FOUND;
				}

				if (response.StartsWith("ERROR:"))
				{
					// Throw exception immediately.
					throw new AerospikeException(command + " failed: " + response);
				}

				string find = "status=";
				int index = response.IndexOf(find);

				if (index < 0)
				{
					// Store exception and keep waiting.
					throw new AerospikeException(command + " failed: " + response);
				}

				int begin = index + find.Length;
				int end = response.IndexOf(':', begin);
				string status = response.Substring(begin, end - begin);

				// Newer servers use "done" while older servers use "DONE"
				if (!status.StartsWith("done", System.StringComparison.OrdinalIgnoreCase))
				{
					return BaseTask.IN_PROGRESS;
				}
			}
			return BaseTask.COMPLETE;
		}
	}
}
