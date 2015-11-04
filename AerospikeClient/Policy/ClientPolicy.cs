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
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Client initialization arguments.
	/// </summary>
	public class ClientPolicy
	{
		/// <summary>
		/// User authentication to cluster.  Leave null for clusters running without restricted access.
		/// </summary>
		public string user;

		/// <summary>
		/// Password authentication to cluster.  The password will be stored by the client and sent to server
		/// in hashed format.  Leave null for clusters running without restricted access.
		/// </summary>
		public string password;

		/// <summary>
		/// Initial host connection timeout in milliseconds.  The timeout when opening a connection 
		/// to the server host for the first time.
		/// Default: 1000ms
		/// </summary>
		public int timeout = 1000;

		/// <summary>
		/// Estimate of incoming threads concurrently using synchronous methods in the client instance.
		/// This field is used to size the synchronous connection pool for each server node.
		/// Default: 300
		/// </summary>
		public int maxThreads = 300;

		/// <summary>
		/// Maximum socket idle in seconds.  Socket connection pools will discard sockets
		/// that have been idle longer than the maximum.  The value is limited to 24 hours (86400).
		/// <para>
		/// It's important to set this value to a few seconds less than the server's proto-fd-idle-ms
		/// (default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket 
		/// that has already been reaped by the server.
		/// Default: 55 seconds
		/// </para>
		/// </summary>
		public int maxSocketIdle = 55;

		/// <summary>
		/// Interval in milliseconds between cluster tends by maintenance thread.  Default: 1 second
		/// </summary>
		public int tendInterval = 1000;
	
		/// <summary>
		/// Throw exception if all seed connections fail on cluster instantiation.  Default: true
		/// </summary>
		public bool failIfNotConnected = true;

		/// <summary>
		/// Default read policy that is used when read command's policy is null.
		/// </summary>
		public Policy readPolicyDefault = new Policy();

		/// <summary>
		/// Default write policy that is used when write command's policy is null.
		/// </summary>
		public WritePolicy writePolicyDefault = new WritePolicy();

		/// <summary>
		/// Default scan policy that is used when scan command's policy is null.
		/// </summary>
		public ScanPolicy scanPolicyDefault = new ScanPolicy();

		/// <summary>
		/// Default query policy that is used when query command's policy is null.
		/// </summary>
		public QueryPolicy queryPolicyDefault = new QueryPolicy();

		/// <summary>
		/// Default batch policy that is used when batch command's policy is null.
		/// </summary>
		public BatchPolicy batchPolicyDefault = new BatchPolicy();

		/// <summary>
		/// Default info policy that is used when info command's policy is null.
		/// </summary>
		public InfoPolicy infoPolicyDefault = new InfoPolicy();

		/// <summary>
		/// A IP translation table is used in cases where different clients use different server 
		/// IP addresses.  This may be necessary when using clients from both inside and outside 
		/// a local area network.  Default is no translation.
		/// <para>
		/// The key is the IP address returned from friend info requests to other servers.  The 
		/// value is the real IP address used to connect to the server.
		/// </para>
		/// </summary>
		public Dictionary<string, string> ipMap;

		/// <summary>
		/// Should prole replicas be requested from each server node in the cluster tend thread.
		/// This option is required if there is a need to distribute reads across proles.
		/// (<seealso cref="Aerospike.Client.Policy.replica"/> == <seealso cref="Aerospike.Client.Replica.MASTER_PROLES"/>).
		/// <para> 
		/// If requestProleReplicas is enabled, all prole partition maps will be cached on the client which results in 
		/// extra storage multiplied by the replication factor.
		/// </para>
		/// <para>
		/// The default is false (only request master replicas and never prole replicas).
		/// </para>
		/// </summary>
		public bool requestProleReplicas;
	}
}
