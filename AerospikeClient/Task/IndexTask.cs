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
	/// Task used to poll for long running create index completion.
	/// </summary>
	public sealed class IndexTask : Task
	{
		private readonly string ns;
		private readonly string indexName;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public IndexTask(Cluster cluster, string ns, string indexName) 
			: base(cluster, false)
		{
			this.ns = ns;
			this.indexName = indexName;
		}

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public IndexTask() 
			: base(null, true)
		{
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override bool IsDone()
		{
			string command = "sindex/" + ns + '/' + indexName;
			Node[] nodes = cluster.Nodes;
			bool complete = false;
    
			foreach (Node node in nodes)
			{
				try
				{
					string response = Info.Request(node, command);
					string find = "load_pct=";
					int index = response.IndexOf(find);
    
					if (index < 0)
					{
						complete = true;
						continue;
					}
    
					int begin = index + find.Length;
					int end = response.IndexOf(';', begin);
					string str = response.Substring(begin, end - begin);
					int pct = Convert.ToInt32(str);
    
					if (pct >= 0 && pct < 100)
					{
						return false;
					}
					complete = true;
				}
				catch (Exception)
				{
					complete = true;
				}
			}
			return complete;
		}
	}
}
