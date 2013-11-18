/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
		public override bool IsDone()
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