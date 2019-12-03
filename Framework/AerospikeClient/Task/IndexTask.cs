/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
		private readonly bool isCreate;
		private String statusCommand;
		private String existsCommand;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public IndexTask(Cluster cluster, Policy policy, string ns, string indexName, bool isCreate)
			: base(cluster, policy)
		{
			this.ns = ns;
			this.indexName = indexName;
			this.isCreate = isCreate;
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override int QueryStatus()
		{
			// All nodes must respond with complete to be considered done.
			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException("Cluster is empty");
			}

			foreach (Node node in nodes)
			{
				if (isCreate || !node.HasIndexExists)
				{
					// Check index status.
					if (statusCommand == null)
					{
						statusCommand = BuildStatusCommand(ns, indexName);
					}

					string response = Info.Request(policy, node, statusCommand);
					int status = ParseStatusResponse(statusCommand, response, isCreate);

					if (status != BaseTask.COMPLETE)
					{
						return status;
					}
				}
				else
				{
					// Check if index exists.
					if (existsCommand == null)
					{
						existsCommand = BuildExistsCommand(ns, indexName);
					}

					string response = Info.Request(policy, node, existsCommand);
					int status = ParseExistsResponse(existsCommand, response);

					if (status != BaseTask.COMPLETE)
					{
						return status;
					}
				}
			}
			return BaseTask.COMPLETE;
		}

		public static string BuildStatusCommand(string ns, string indexName)
		{
			return "sindex/" + ns + '/' + indexName;
		}

		public static int ParseStatusResponse(string command, string response, bool isCreate)
		{
			if (isCreate)
			{
				// Check if index has been created.
				string find = "load_pct=";
				int index = response.IndexOf(find);

				if (index < 0)
				{
					if (response.IndexOf("FAIL:201") >= 0 || response.IndexOf("FAIL:203") >= 0)
					{
						// Index not found or not readable.
						return BaseTask.NOT_FOUND;
					}
					else
					{
						// Throw exception immediately.
						throw new AerospikeException(command + " failed: " + response);
					}
				}

				int begin = index + find.Length;
				int end = response.IndexOf(';', begin);
				string str = response.Substring(begin, end - begin);
				int pct = Convert.ToInt32(str);

				if (pct != 100)
				{
					return BaseTask.IN_PROGRESS;
				}
			}
			else
			{
				// Check if index has been dropped.
				if (response.IndexOf("FAIL:201") < 0)
				{
					// Index still exists.
					return BaseTask.IN_PROGRESS;
				}
			}
			return BaseTask.COMPLETE;
		}

		public static string BuildExistsCommand(string ns, string indexName)
		{
			return "sindex-exists:ns=" + ns + ";indexname=" + indexName;
		}

		public static int ParseExistsResponse(string command, string response)
		{
			if (response.Equals("false"))
			{
				return BaseTask.COMPLETE;
			}

			if (response.Equals("true"))
			{
				return BaseTask.IN_PROGRESS;
			}

			throw new AerospikeException(command + " failed: " + response);
		}
	}
}
