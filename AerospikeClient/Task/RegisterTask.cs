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