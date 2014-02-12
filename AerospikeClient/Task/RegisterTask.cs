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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Task used to poll for UDF registration completion.
	/// </summary>
	public sealed class RegisterTask : Task
	{
		private readonly string packageName;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public RegisterTask(Cluster cluster, string packageName)
			: base(cluster, false)
		{
			this.packageName = packageName;
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override bool IsDone()
		{
			string command = "udf-list";
			Node[] nodes = cluster.Nodes;
			bool done = false;

			foreach (Node node in nodes)
			{
				string response = Info.Request(node, command);
				string find = "filename=" + packageName;
				int index = response.IndexOf(find);

				if (index < 0)
				{
					return false;
				}
				done = true;
			}
			return done;
		}
	}
}
