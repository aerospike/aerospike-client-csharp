/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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