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
		public override bool QueryIfDone()
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
