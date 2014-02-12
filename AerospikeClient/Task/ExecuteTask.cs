/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
