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
namespace Aerospike.Client
{
	/// <summary>
	/// Task used to poll for long running execute job completion.
	/// </summary>
	public sealed class ExecuteTask : Task
	{
		private readonly int taskId;
		private readonly bool scan;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public ExecuteTask(Cluster cluster, Statement statement)
			: base(cluster, false)
		{
			this.taskId = statement.taskId;
			this.scan = statement.filters == null;
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override bool QueryIfDone()
		{
			string command = (scan) ? "scan-list" : "query-list";
			Node[] nodes = cluster.Nodes;
			bool done = false;

			foreach (Node node in nodes)
			{
				string response = Info.Request(node, command);
				string find = "job_id=" + taskId + ':';
				int index = response.IndexOf(find);

				if (index < 0)
				{
					done = true;
					continue;
				}

				int begin = index + find.Length;
				find = "job_status=";
				index = response.IndexOf(find, begin);

				if (index < 0)
				{
					continue;
				}

				begin = index + find.Length;
				int end = response.IndexOf(':', begin);
				string status = response.Substring(begin, end - begin);
				
				if (status.Equals("ABORTED"))
				{
					throw new AerospikeException.QueryTerminated();
				}
				else if (status.Equals("IN PROGRESS"))
				{
					return false;
				}
				else if (status.Equals("DONE"))
				{
					done = true;
				}
			}
			return done;
		}
	}
}
