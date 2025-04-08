/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

using System.Diagnostics;

namespace Aerospike.Client
{
	/// <summary>
	/// Client initialization arguments.
	/// </summary>
	public class ClientPolicy
	{
		/// <summary>
		/// User authentication to cluster.  Leave null for clusters running without restricted access.
		/// <para>Default: null</para>
		/// </summary>
		public string user;

		/// <summary>
		/// Password authentication to cluster.  The password will be stored by the client and sent to server
		/// in hashed format.  Leave null for clusters running without restricted access.
		/// <para>Default: null</para>
		/// </summary>
		public string password;

		/// <summary>
		/// Expected cluster name.  If populated, the clusterName must match the cluster-name field
		/// in the service section in each server configuration.  This ensures that the specified
		/// seed nodes belong to the expected cluster on startup.  If not, the client will refuse
		/// to add the node to the client's view of the cluster.
		/// <para>Default: null</para>
		/// </summary>
		public string clusterName;

		/// <summary>
		/// Authentication mode.
		/// <para>Default: AuthMode.INTERNAL</para>
		/// </summary>
		public AuthMode authMode = AuthMode.INTERNAL;

		/// <summary>
		/// Initial host connection timeout in milliseconds.  The timeout when opening a connection 
		/// to the server host for the first time.
		/// <para>Default: 1000</para>
		/// </summary>
		public int timeout = 1000;

		/// <summary>
		/// Login timeout in milliseconds.  The timeout used when user authentication is enabled and
		/// a node login is being performed.
		/// <para>Default: 5000</para>
		/// </summary>
		public int loginTimeout = 5000;

		/// <summary>
		/// Minimum number of synchronous connections allowed per server node.  Preallocate min connections
		/// on client node creation.  The client will periodically allocate new connections if count falls
		/// below min connections.
		/// <para>
		/// Server proto-fd-idle-ms and client <see cref="Aerospike.Client.ClientPolicy.maxSocketIdle"/>
		/// should be set to zero (no reap) if minConnsPerNode is greater than zero.  Reaping connections
		/// can defeat the purpose of keeping connections in reserve for a future burst of activity.
		/// </para>
		/// <para>Default: 0</para>
		/// </summary>
		public int minConnsPerNode;

		/// <summary>
		/// Maximum number of synchronous connections allowed per server node.  Commands will go
		/// through retry logic and potentially fail with "ResultCode.NO_MORE_CONNECTIONS" if the maximum
		/// number of connections would be exceeded.
		/// <para>
		/// The number of connections used per node depends on how many concurrent threads issue
		/// database commands plus sub-threads used for parallel multi-node commands (batch, scan,
		/// and query). One connection will be used for each thread.
		/// </para>
		/// <para>
		/// See <see cref="AsyncClientPolicy.asyncMaxConnsPerNode"/> to configure max connections for
		/// asynchronous commands.
		/// </para>
		/// <para>Default: 100</para>
		/// </summary>
		public int maxConnsPerNode = 100;

		/// <summary>
		/// Number of synchronous connection pools used for each node.  Machines with 8 cpu cores or
		/// less usually need just one connection pool per node.  Machines with a large number of cpu
		/// cores may have their synchronous performance limited by contention for pooled connections.
		/// Contention for pooled connections can be reduced by creating multiple mini connection pools
		/// per node.
		/// <para>Default: 1</para>
		/// </summary>
		public int connPoolsPerNode = 1;

		/// <summary>
		/// Maximum socket idle in seconds.  Socket connection pools will discard sockets
		/// that have been idle longer than the maximum.
		/// <para>
		/// Connection pools are now implemented by a LIFO stack.  Connections at the tail of the
		/// stack will always be the least used.  These connections are checked for maxSocketIdle
		/// once every 30 tend iterations (usually 30 seconds).
		/// </para>
		/// <para>
		/// If server's proto-fd-idle-ms is greater than zero, then maxSocketIdle should be
		/// at least a few seconds less than the server's proto-fd-idle-ms, so the client does not
		/// attempt to use a socket that has already been reaped by the server.
		/// </para>
		/// <para>
		/// If server's proto-fd-idle-ms is zero (no reap), then maxSocketIdle should also be zero.
		/// Connections retrieved from a pool in commands will not be checked for maxSocketIdle
		/// when maxSocketIdle is zero.  Idle connections will still be trimmed down from peak
		/// connections to min connections (minConnsPerNode and asyncMinConnsPerNode) using a
		/// hard-coded 55 second limit in the cluster tend thread.
		/// </para>
		/// <para>Default: 0</para>
		/// </summary>
		public int maxSocketIdle;

		/// <summary>
		/// Maximum number of errors allowed per node per <see cref="errorRateWindow"/> before backoff
		/// algorithm throws <see cref="Aerospike.Client.AerospikeException.Backoff"/> on database
		/// commands to that node. If maxErrorRate is zero, there is no error limit and
		/// the exception will not be thrown.
		/// <para>
		/// The counted error types are any error that causes the connection to close (socket errors
		/// and client timeouts) and <see cref="Aerospike.Client.ResultCode.DEVICE_OVERLOAD"/>.
		/// </para>
		/// <para>
		/// Default: 100
		/// </para>
		/// </summary>
		public int maxErrorRate = 100;

		/// <summary>
		/// The number of cluster tend iterations that defines the window for <see cref="maxErrorRate"/>.
		/// One tend iteration is defined as <see cref="tendInterval"/> plus the time to tend all nodes.
		/// At the end of the window, the error count is reset to zero and backoff State is removed
		/// on all nodes.
		/// <para>
		/// Default: 1
		/// </para>
		/// </summary>
		public int errorRateWindow = 1;

		/// <summary>
		/// Interval in milliseconds between cluster tends by maintenance thread.
		/// <para>Default: 1000</para>
		/// </summary>
		public int tendInterval = 1000;

		/// <summary>
		/// Should cluster instantiation fail if the client fails to connect to a seed or
		/// all the seed's peers.
		/// <para>
		/// If true, throw an exception if all seed connections fail or a seed is valid,
		/// but all peers from that seed are not reachable.
		/// </para>
		/// <para>
		/// If false, a partial cluster will be created and the client will automatically connect
		/// to the remaining nodes when they become available.
		/// </para>
		/// <para>
		/// Default: true
		/// </para>
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
		///  Default parent policy used in batch read commands. Parent policy fields
		///  include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy batchPolicyDefault = BatchPolicy.ReadDefault();

		/// <summary>
		/// Default parent policy used in batch write commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy batchParentPolicyWriteDefault = BatchPolicy.WriteDefault();

		/// <summary>
		/// Default write policy used in batch operate commands.
		/// Write policy fields include generation, expiration, durableDelete, etc...
		/// </summary>
		public BatchWritePolicy batchWritePolicyDefault = new BatchWritePolicy();

		/// <summary>
		/// Default delete policy used in batch delete commands.
		/// </summary>
		public BatchDeletePolicy batchDeletePolicyDefault = new BatchDeletePolicy();

		/// <summary>
		/// Default user defined function policy used in batch UDF excecute commands.
		/// </summary>
		public BatchUDFPolicy batchUDFPolicyDefault = new BatchUDFPolicy();

		/// <summary>
		/// Default transactions policy when verifying record versions in a batch on a commit.
		/// </summary>
		public TxnVerifyPolicy txnVerifyPolicyDefault = new TxnVerifyPolicy();

		/// <summary>
		/// Default transactions policy when rolling the transaction records forward (commit)
		/// or back(abort) in a batch.
		/// </summary>
		public TxnRollPolicy txnRollPolicyDefault = new TxnRollPolicy();

		/// <summary>
		/// Default info policy that is used when info command's policy is null.
		/// </summary>
		public InfoPolicy infoPolicyDefault = new InfoPolicy();

		/// <summary>
		/// Secure connection policy for servers that require TLS connections.
		/// Secure connections are only supported for AerospikeClient synchronous commands.
		/// <para>
		/// Secure connections are not supported for asynchronous commands because AsyncClient 
		/// uses the best performing SocketAsyncEventArgs.  Unfortunately, SocketAsyncEventArgs is
		/// not supported by the provided SslStream.
		/// </para>
		/// <para>Default: null (Use normal sockets)</para>
		/// </summary>
		public TlsPolicy tlsPolicy;

		/// <summary>
		/// A IP translation table is used in cases where different clients use different server 
		/// IP addresses.  This may be necessary when using clients from both inside and outside 
		/// a local area network.  Default is no translation.
		/// <para>
		/// The key is the IP address returned from friend info requests to other servers.  The 
		/// value is the real IP address used to connect to the server.
		/// </para>
		/// <para>Default: null (no IP address translation)</para>
		/// </summary>
		public Dictionary<string, string> ipMap;

		/// <summary>
		/// Should use "services-alternate" instead of "services" in info request during cluster
		/// tending.  "services-alternate" returns server configured external IP addresses that client
		/// uses to talk to nodes.  "services-alternate" can be used in place of providing a client "ipMap".
		/// <para>Default: false (use original "services" info request)</para>
		/// </summary>
		public bool useServicesAlternate;

		/// For testing purposes only.  Do not modify.
		/// <para>
		/// Should the AerospikeClient instance communicate with the first seed node only
		/// instead of using the data partition map to determine which node to send the
		/// database command.
		/// </para>
		/// Default: false
		public bool forceSingleNode = false;

		/// <summary>
		/// Track server rack data.  This field is useful when directing read commands to the server node
		/// that contains the key and exists on the same rack as the client.  This serves to lower cloud
		/// provider costs when nodes are distributed across different racks/data centers.
		/// <para>
		/// <see cref="Aerospike.Client.ClientPolicy.rackId"/> or <see cref="Aerospike.Client.ClientPolicy.rackIds"/>, 
		/// <see cref="Aerospike.Client.Replica.PREFER_RACK"/> and server rack configuration must also be set to
		/// enable this functionality.
		/// </para>
		/// <para>Default: false</para>
		/// </summary>
		public bool rackAware;

		/// <summary>
		/// Rack where this client instance resides. If <see cref="Aerospike.Client.ClientPolicy.rackIds"/> is set,
		/// rackId is ignored.
		/// <para>
		/// <see cref="Aerospike.Client.ClientPolicy.rackAware"/>, <see cref="Aerospike.Client.Replica.PREFER_RACK"/>
		/// and server rack configuration must also be set to enable this functionality.
		/// </para>
		/// <para>Default: 0</para>
		/// </summary>
		public int rackId;

		/// <summary>
		/// List of acceptable racks in order of preference.
		/// If rackIds is set, <see cref="Aerospike.Client.ClientPolicy.rackId"/> is ignored.
		/// <para>
		/// <see cref="Aerospike.Client.ClientPolicy.rackAware"/>, <see cref="Aerospike.Client.Replica.PREFER_RACK"/>
		/// and server rack configuration must also be set to enable this functionality.
		/// </para>
		/// <para>Default: null</para>
		/// </summary>
		public List<int> rackIds;

		/// <summary>
		/// Dynamic configuration provider.
		/// </summary>
		public IAerospikeConfigProvider ConfigProvider = null;
		
		/// <summary>
		/// Copy client policy from another client policy.
		/// </summary>
		public ClientPolicy(ClientPolicy other)
		{
			this.user = other.user;
			this.password = other.password;
			this.clusterName = other.clusterName;
			this.authMode = other.authMode;
			this.timeout = other.timeout;
			this.loginTimeout = other.loginTimeout;
			this.minConnsPerNode = other.minConnsPerNode;
			this.maxConnsPerNode = other.maxConnsPerNode;
			this.connPoolsPerNode = other.connPoolsPerNode;
			this.maxSocketIdle = other.maxSocketIdle;
			this.maxErrorRate = other.maxErrorRate;
			this.errorRateWindow = other.errorRateWindow;
			this.tendInterval = other.tendInterval;
			this.failIfNotConnected = other.failIfNotConnected;
			this.readPolicyDefault = new Policy(other.readPolicyDefault);
			this.writePolicyDefault = new WritePolicy(other.writePolicyDefault);
			this.scanPolicyDefault = new ScanPolicy(other.scanPolicyDefault);
			this.queryPolicyDefault = new QueryPolicy(other.queryPolicyDefault);
			this.batchPolicyDefault = new BatchPolicy(other.batchPolicyDefault);
			this.batchParentPolicyWriteDefault = new BatchPolicy(other.batchParentPolicyWriteDefault);
			this.batchWritePolicyDefault = new BatchWritePolicy(other.batchWritePolicyDefault);
			this.batchDeletePolicyDefault = new BatchDeletePolicy(other.batchDeletePolicyDefault);
			this.batchUDFPolicyDefault = new BatchUDFPolicy(other.batchUDFPolicyDefault);
			this.txnVerifyPolicyDefault = new TxnVerifyPolicy(other.txnVerifyPolicyDefault);
			this.txnRollPolicyDefault = new TxnRollPolicy(other.txnRollPolicyDefault);
			this.infoPolicyDefault = new InfoPolicy(other.infoPolicyDefault);
			this.tlsPolicy = (other.tlsPolicy != null) ? new TlsPolicy(other.tlsPolicy) : null;
			this.ipMap = other.ipMap;
			this.useServicesAlternate = other.useServicesAlternate;
			this.forceSingleNode = other.forceSingleNode;
			this.rackAware = other.rackAware;
			this.rackId = other.rackId;
			this.rackIds = (other.rackIds != null) ? new List<int>(other.rackIds) : null;
			this.ConfigProvider = other.ConfigProvider;
		}

		public ClientPolicy(ClientPolicy other, IAerospikeConfigProvider configProvider) : this(other)
        {
            var staticClient = ConfigProvider.ConfigurationData.staticProperties.client;
            var dynamicClient = ConfigProvider.ConfigurationData.dynamicProperties.client;

            if (staticClient.max_connections_per_node.HasValue)
            {
                this.maxConnsPerNode = staticClient.max_connections_per_node.Value;
            }
            if (staticClient.min_connections_per_node.HasValue)
            {
                this.minConnsPerNode = staticClient.min_connections_per_node.Value;
            }

            if (dynamicClient.timeout.HasValue)
            {
                this.timeout = dynamicClient.timeout.Value;
            }
            if (dynamicClient.error_rate_window.HasValue)
            {
                this.errorRateWindow = dynamicClient.error_rate_window.Value;
            }
            if (dynamicClient.max_error_rate.HasValue)
            {
                this.maxErrorRate = dynamicClient.max_error_rate.Value;
            }
            if (dynamicClient.fail_if_not_connected.HasValue)
            {
                this.failIfNotConnected = dynamicClient.fail_if_not_connected.Value;
            }
            if (dynamicClient.login_timeout.HasValue)
            {
                this.loginTimeout = dynamicClient.login_timeout.Value;
            }
            if (dynamicClient.max_socket_idle.HasValue)
            {
                this.maxSocketIdle = dynamicClient.max_socket_idle.Value;
            }
            if (dynamicClient.rack_aware.HasValue)
            {
                this.rackAware = dynamicClient.rack_aware.Value;
            }
            if (dynamicClient.rack_ids != null)
            {
                this.rackIds = dynamicClient.rack_ids.ToList();
            }
            if (dynamicClient.tend_interval.HasValue)
            {
                this.tendInterval = dynamicClient.tend_interval.Value;
            }
            if (dynamicClient.use_service_alternative.HasValue)
            {
                this.useServicesAlternate = dynamicClient.use_service_alternative.Value;
            }
        }

        /// <summary>
        /// Default constructor.
        /// </summary>
        public ClientPolicy()
		{
		}

		/// <summary>
		/// Creates a deep copy of this client policy.
		/// </summary>
		/// <returns></returns>
		public ClientPolicy Clone()
		{
			return new ClientPolicy(this);
		}
	}
}
