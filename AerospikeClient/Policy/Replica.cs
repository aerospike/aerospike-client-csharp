/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
	/// Defines type of node partition targeted by read commands.
	/// </summary>
	public enum Replica
	{
		/// <summary>
		/// Read from node containing key's master partition.  This is the default behavior.
		/// </summary>
		MASTER,

		/// <summary>
		/// Distribute reads across nodes containing key's master and replicated partitions
		/// in round-robin fashion.  This option requires <seealso cref="Aerospike.Client.ClientPolicy.requestProleReplicas"/>
		/// to be enabled in order to function properly.
		/// </summary>
		MASTER_PROLES,

		/// <summary>
		/// Always try node containing master partition first. If connection fails and
		/// <seealso cref="Aerospike.Client.Policy.retryOnTimeout"/> is true, try nodes
		/// containing prole partition.  This option requires <seealso cref="Aerospike.Client.ClientPolicy.requestProleReplicas"/>
		/// to be enabled in order to function properly.
		/// </summary>
		SEQUENCE,

		/// <summary>
		/// Distribute reads across all nodes in cluster in round-robin fashion.
		/// This option is useful when the replication factor equals the number
		/// of nodes in the cluster and the overhead of requesting proles is not desired.
		/// </summary>
		RANDOM
	}
}
