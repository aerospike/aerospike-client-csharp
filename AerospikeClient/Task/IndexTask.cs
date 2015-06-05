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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Task used to poll for long running create index completion.
	/// </summary>
	public sealed class IndexTask : BaseTask
	{
		private readonly string ns;
		private readonly string indexName;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public IndexTask(Cluster cluster, Policy policy, string ns, string indexName)
			: base(cluster, policy)
		{
			this.ns = ns;
			this.indexName = indexName;
		}

		/// <summary>
		/// Initialize task that has already completed.
		/// </summary>
		public IndexTask() 
		{
			ns = null;
			indexName = null;
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override bool QueryIfDone()
		{
			string command = "sindex/" + ns + '/' + indexName;
			Node[] nodes = cluster.Nodes;
			bool complete = false;
    
			foreach (Node node in nodes)
			{
				try
				{
					string response = Info.Request(policy, node, command);
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
