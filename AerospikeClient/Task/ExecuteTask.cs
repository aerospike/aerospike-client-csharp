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
namespace Aerospike.Client
{
	/// <summary>
	/// Task used to poll for long running execute job completion.
	/// </summary>
	public sealed class ExecuteTask : BaseTask
	{
		private readonly long taskId;
		private readonly bool scan;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public ExecuteTask(Cluster cluster, Policy policy, Statement statement)
			: base(cluster, policy)
		{
			this.taskId = statement.taskId;
			this.scan = statement.filters == null;
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override bool QueryIfDone()
		{
			string module = (scan) ? "scan" : "query";
			string command = "jobs:module=" + module + ";cmd=get-job;trid=" + taskId;
			Node[] nodes = cluster.Nodes;
			bool done = false;

			foreach (Node node in nodes)
			{
				string response = Info.Request(policy, node, command);

				if (response.StartsWith("ERROR:"))
				{
					done = true;
					continue;
				}

				string find = "status=";
				int index = response.IndexOf(find);

				if (index < 0)
				{
					continue;
				}

				int begin = index + find.Length;
				int end = response.IndexOf(':', begin);
				string status = response.Substring(begin, end - begin);

				if (status.Equals("active"))
				{
					return false;
				}
				else if (status.StartsWith("done"))
				{
					done = true;
				}
			}
			return done;
		}
	}
}
