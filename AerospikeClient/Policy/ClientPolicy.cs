/* 
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;
using Aerospike.Client.Config;

namespace Aerospike.Client
{
	/// <summary>
	/// Client initialization arguments.
	/// </summary>
	public class ClientPolicy
	{
		private const string DiscoveryProviderPath = "static.client.discovery.provider";
		private const string TranslationProviderPath = "static.client.translation.provider";

		private Dictionary<string, Func<IDictionary<string, object>, ISeedCandidateProvider>> seedCandidateProviderFactories;
		private Dictionary<string, Func<IDictionary<string, object>, IAddressTranslator>> addressTranslatorFactories;
		private Registered<ISeedCandidateProvider> registeredSeedCandidateProvider;
		private Registered<IAddressTranslator> registeredAddressTranslator;

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
		/// Application identifier.
		/// <para>Default: null</para>
		/// </summary>
		public string AppId;

		/// <summary>
		/// Authentication mode.
		/// <see cref="AuthMode.EXTERNAL"/> and <see cref="AuthMode.PKI"/> require TLS.
		/// PKI authentication also requires a client certificate and does not use a password.
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
		/// <para>
		/// Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
		/// the server will stop automatically reaping based on socket idle timeouts.
		/// </para>
		/// <para>Default: 0</para>
		/// </summary>
		public int minConnsPerNode;

		/// <summary>
		/// Maximum number of synchronous connections allowed per server node. Commands go through
		/// retry logic and can fail with <see cref="ResultCode.NO_MORE_CONNECTIONS"/> when the pool is
		/// exhausted. This limit is per node, not cluster-wide.
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
		/// <para>
		/// Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
		/// the server will stop automatically reaping based on socket idle timeouts.
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
		/// If false, the client can start with no seed connection or with a partial cluster view.
		/// The cluster tend thread will continue attempting discovery and connect to nodes when
		/// they become available. Database commands can fail until a suitable node is discovered.
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
		/// Secure connections are supported for both synchronous and asynchronous commands.
		/// TLS is required by <see cref="AuthMode.EXTERNAL"/> and <see cref="AuthMode.PKI"/>.
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
		/// <para>
		/// When <see cref="addressTranslator"/> is null, this map backs the default
		/// <see cref="StaticMapAddressTranslator"/>. When <see cref="addressTranslator"/> is set,
		/// this field is ignored.
		/// </para>
		/// <para>Default: null (no IP address translation)</para>
		/// </summary>
		public Dictionary<string, string> ipMap;

		/// <summary>
		/// Source of seed hosts used to discover the cluster. When null,
		/// <see cref="StaticSeedCandidateProvider"/> is used with the hosts passed to the client
		/// constructor. Built-in providers set in code include
		/// <see cref="StaticSeedCandidateProvider"/> and <see cref="DnsSeedCandidateProvider"/>
		/// (YAML <c>static.client.discovery.provider: dns</c>). Custom providers can be named with
		/// <see cref="RegisterSeedCandidateProvider"/>.
		/// <para>Default: null</para>
		/// </summary>
		public ISeedCandidateProvider seedCandidateProvider;

		/// <summary>
		/// Translator applied to every server advertised address before the client connects to it.
		/// When set, <see cref="ipMap"/> is ignored. When null,
		/// <see cref="StaticMapAddressTranslator"/> is used with <see cref="ipMap"/>.
		/// Built-in translators: <see cref="StaticMapAddressTranslator"/> (<c>static-map</c>),
		/// <see cref="PortMapAddressTranslator"/> (<c>port-map</c>),
		/// <see cref="PassThroughAddressTranslator"/> (<c>pass-through</c>). Custom translators can
		/// be named with <see cref="RegisterAddressTranslator"/>.
		/// <para>Default: null</para>
		/// </summary>
		public IAddressTranslator addressTranslator;

		/// <summary>
		/// Action taken when the address translator has no mapping for a server advertised endpoint.
		/// <para>Default: <see cref="TranslationMissMode.PASS_THROUGH"/></para>
		/// </summary>
		public TranslationMissMode translationOnMiss = TranslationMissMode.PASS_THROUGH;

		/// <summary>
		/// How static constructor seeds and discovered seeds are combined into the seed list.
		/// <para>Default: <see cref="SeedMergePolicy.MERGE"/></para>
		/// </summary>
		public SeedMergePolicy seedMergePolicy = SeedMergePolicy.MERGE;

		/// <summary>
		/// Where periodic seed refresh runs when the provider needs periodic refresh (FR-41).
		/// <para>Default: <see cref="DiscoveryExecutionMode.THREAD"/></para>
		/// </summary>
		public DiscoveryExecutionMode discoveryExecutionMode = DiscoveryExecutionMode.THREAD;

		/// <summary>
		/// Interval in milliseconds between periodic seed candidate refreshes.
		/// Must be greater or equal to <see cref="tendInterval"/> when the provider needs periodic refresh.
		/// <para>Default: 30000</para>
		/// </summary>
		public int discoveryRefreshIntervalMs = 30000;

		/// <summary>
		/// Max wall time in milliseconds for one <see cref="ISeedCandidateProvider.RefreshSeedCandidates"/>
		/// call in <see cref="DiscoveryExecutionMode.THREAD"/> mode (initial await and close wait).
		/// <para>Default: 10000</para>
		/// </summary>
		public int discoveryRefreshTimeoutMs = 10000;

		/// <summary>
		/// Declared deadline in milliseconds for one periodic refresh on the tend thread
		/// (<see cref="DiscoveryExecutionMode.TEND"/>). Measured, not enforced.
		/// Must be in the range <c>0 &lt; value &lt;= tendInterval / 2</c>.
		/// <para>Default: 100</para>
		/// </summary>
		public int discoveryTendRefreshDeadlineMs = 100;

		/// <summary>
		/// Flag to signify if alternate IP address discovery info commands should be used.
		/// Enable this when the addresses advertised by standard service/peer commands are not
		/// reachable from the client, such as clients outside a container or private network.
		/// 
		/// If false, use:
		/// IP address: service-clear-std
		/// TLS IP address: service-tls-std
		/// Peers addresses: peers-clear-std
		/// Peers TLS addresses: peers-tls-std
		/// 
		/// If true, use:
		/// IP address: service-clear-alt
		/// TLS IP address: service-tls-alt
		/// Peers addresses: peers-clear-alt
		/// Peers TLS addresses: peers-tls-alt
		/// 
		/// Default: false
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
		/// Copy client policy from another client policy.
		/// </summary>
		public ClientPolicy(ClientPolicy other)
		{
			this.user = other.user;
			this.password = other.password;
			this.clusterName = other.clusterName;
			this.AppId = other.AppId;
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
			this.seedCandidateProvider = other.seedCandidateProvider;
			this.addressTranslator = other.addressTranslator;
			this.translationOnMiss = other.translationOnMiss;
			this.seedMergePolicy = other.seedMergePolicy;
			this.discoveryExecutionMode = other.discoveryExecutionMode;
			this.discoveryRefreshIntervalMs = other.discoveryRefreshIntervalMs;
			this.discoveryRefreshTimeoutMs = other.discoveryRefreshTimeoutMs;
			this.discoveryTendRefreshDeadlineMs = other.discoveryTendRefreshDeadlineMs;
			this.useServicesAlternate = other.useServicesAlternate;
			this.forceSingleNode = other.forceSingleNode;
			this.rackAware = other.rackAware;
			this.rackId = other.rackId;
			this.rackIds = (other.rackIds != null) ? new List<int>(other.rackIds) : null;
			this.seedCandidateProviderFactories = (other.seedCandidateProviderFactories != null) ?
				new Dictionary<string, Func<IDictionary<string, object>, ISeedCandidateProvider>>(other.seedCandidateProviderFactories) : null;
			this.addressTranslatorFactories = (other.addressTranslatorFactories != null) ?
				new Dictionary<string, Func<IDictionary<string, object>, IAddressTranslator>>(other.addressTranslatorFactories) : null;
			this.registeredSeedCandidateProvider = other.registeredSeedCandidateProvider;
			this.registeredAddressTranslator = other.registeredAddressTranslator;
		}

		public ClientPolicy(ClientPolicy other, IConfigProvider configProvider) : this(other)
		{
			if (configProvider == null)
			{
				return;
			}

			if (configProvider.ConfigurationData == null)
			{
				return;
			}

			var staticConfig = configProvider.ConfigurationData.staticConfig;
			if (staticConfig == null)
			{
				return;
			}

			var staticClient = configProvider.ConfigurationData.staticConfig.client;
			if (staticClient == null)
			{
				return;
			}

			var dynamicConfig = configProvider.ConfigurationData.dynamicConfig;
			if (dynamicConfig == null)
			{
				return;
			}

			var dynamicClient = configProvider.ConfigurationData.dynamicConfig.client;
			if (dynamicClient == null)
			{
				return;
			}

			if (staticClient.max_connections_per_node.HasValue)
			{
				this.maxConnsPerNode = staticClient.max_connections_per_node.Value;
			}
			if (staticClient.min_connections_per_node.HasValue)
			{
				this.minConnsPerNode = staticClient.min_connections_per_node.Value;
			}

			ApplyDiscoveryAndTranslation(staticClient);

			if (!String.IsNullOrEmpty(dynamicClient.app_id))
			{
				this.AppId = dynamicClient.app_id;
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
				this.rackIds = [.. dynamicClient.rack_ids];
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
		/// Register a seed candidate provider factory under a name. When
		/// <c>static.client.discovery.provider</c> in the YAML config file names it, the client calls
		/// the factory with the other keys of the discovery block as the options map and sets
		/// <see cref="seedCandidateProvider"/> to the result. Built once at client construction and
		/// kept across config reloads.
		/// </summary>
		/// <param name="name">Must not be a built-in name (<c>static</c>, <c>dns</c>, <c>aws-tags</c>) or already registered</param>
		/// <param name="factory">Builds the provider from the options map; must not return null</param>
		public void RegisterSeedCandidateProvider(
			string name,
			Func<IDictionary<string, object>, ISeedCandidateProvider> factory)
		{
			seedCandidateProviderFactories = Register("seed candidate provider", name, factory,
				DiscoveryYaml.BuiltInSeedProviders, seedCandidateProviderFactories);
		}

		/// <summary>
		/// Register an address translator factory under a name. When
		/// <c>static.client.translation.provider</c> in the YAML config file names it, the client calls
		/// the factory with the other keys of the translation block as the options map and sets
		/// <see cref="addressTranslator"/> to the result. Built once at client construction and kept
		/// across config reloads.
		/// </summary>
		/// <param name="name">Must not be a built-in name (<c>static-map</c>, <c>port-map</c>, <c>pass-through</c>) or already registered</param>
		/// <param name="factory">Builds the translator from the options map; must not return null</param>
		public void RegisterAddressTranslator(
			string name,
			Func<IDictionary<string, object>, IAddressTranslator> factory)
		{
			addressTranslatorFactories = Register("address translator", name, factory,
				DiscoveryYaml.BuiltInTranslators, addressTranslatorFactories);
		}

		private void ApplyDiscoveryAndTranslation(StaticClientConfig staticClient)
		{
			if (staticClient.ip_map != null)
			{
				this.ipMap = staticClient.ip_map;
			}

			if (staticClient.use_services_alternate.HasValue)
			{
				this.useServicesAlternate = staticClient.use_services_alternate.Value;
			}

			StaticDiscoveryConfig discovery = staticClient.discovery;

			if (discovery != null)
			{
				if (discovery.provider != null)
				{
					CheckRegistered(DiscoveryProviderPath, discovery.provider, discovery.options,
						DiscoveryYaml.YamlSeedProviders, seedCandidateProviderFactories);
				}

				if (discovery.options != null || discovery.dns != null || discovery.aws != null)
				{
					Registered<ISeedCandidateProvider> registered;

					if (discovery.dns != null)
					{
						registered = BuildDns(discovery.dns, registeredSeedCandidateProvider);
					}
					else if (discovery.aws != null)
					{
						registered = BuildAws(discovery.aws, registeredSeedCandidateProvider);
					}
					else
					{
						registered = Build(DiscoveryProviderPath, discovery.provider, discovery.options,
							seedCandidateProviderFactories, registeredSeedCandidateProvider);
					}

					registeredSeedCandidateProvider = registered;
					seedCandidateProvider = registered.instance;
				}
				else if (discovery.provider != null)
				{
					seedCandidateProvider = null;
				}

				if (discovery.merge_policy != null)
				{
					seedMergePolicy = DiscoveryYaml.ParseMergePolicy(discovery.merge_policy);
				}

				if (discovery.refresh_interval_ms.HasValue)
				{
					discoveryRefreshIntervalMs = discovery.refresh_interval_ms.Value;
				}

				if (discovery.tend_refresh_deadline_ms.HasValue)
				{
					discoveryTendRefreshDeadlineMs = discovery.tend_refresh_deadline_ms.Value;
				}

				if (discovery.execution_mode != null)
				{
					discoveryExecutionMode = DiscoveryYaml.ParseExecutionMode(discovery.execution_mode);
				}

				if (discovery.refresh_timeout_ms.HasValue)
				{
					discoveryRefreshTimeoutMs = discovery.refresh_timeout_ms.Value;
				}
			}

			StaticTranslationConfig translation = staticClient.translation;

			if (translation != null)
			{
				if (translation.provider != null)
				{
					CheckRegistered(TranslationProviderPath, translation.provider, translation.options,
						DiscoveryYaml.BuiltInTranslators, addressTranslatorFactories);

					switch (translation.provider)
					{
						case StaticTranslationConfig.PortMap:
							addressTranslator = new PortMapAddressTranslator(translation.port_map);
							break;

						case StaticTranslationConfig.PassThrough:
							addressTranslator = PassThroughAddressTranslator.Instance;
							break;

						case StaticTranslationConfig.StaticMap:
							addressTranslator = (translation.static_map != null) ?
								new StaticMapAddressTranslator(translation.static_map) : null;
							break;

						default:
							registeredAddressTranslator = Build(TranslationProviderPath, translation.provider,
								translation.options, addressTranslatorFactories, registeredAddressTranslator);
							addressTranslator = registeredAddressTranslator.instance;
							break;
					}
				}

				if (translation.on_miss != null)
				{
					translationOnMiss = DiscoveryYaml.ParseOnMiss(translation.on_miss);
				}
			}
		}

		private static Dictionary<string, Func<IDictionary<string, object>, T>> Register<T>(
			string type,
			string name,
			Func<IDictionary<string, object>, T> factory,
			string[] builtIn,
			Dictionary<string, Func<IDictionary<string, object>, T>> factories)
		{
			if (string.IsNullOrEmpty(name))
			{
				throw new AerospikeException("Invalid " + type + " name: " + name);
			}

			if (Array.IndexOf(builtIn, name) >= 0)
			{
				throw new AerospikeException("Invalid " + type + " name '" + name + "': built-in name");
			}

			if (factories != null && factories.ContainsKey(name))
			{
				throw new AerospikeException("Invalid " + type + " name '" + name + "': already registered");
			}

			if (factory == null)
			{
				throw new AerospikeException("Invalid " + type + " factory for '" + name + "': null");
			}

			factories ??= new Dictionary<string, Func<IDictionary<string, object>, T>>();
			factories[name] = factory;
			return factories;
		}

		private static Registered<T> Build<T>(
			string path,
			string name,
			IDictionary<string, object> options,
			Dictionary<string, Func<IDictionary<string, object>, T>> factories,
			Registered<T> previous)
			where T : class
		{
			if (previous != null)
			{
				return previous;
			}

			T instance;

			try
			{
				instance = factories[name](options);
			}
			catch (Exception e) when (e is not AerospikeException)
			{
				throw new AerospikeException("Invalid " + path + " value '" + name + "': factory failed: " + e.Message, e);
			}

			if (instance == null)
			{
				throw new AerospikeException("Invalid " + path + " value '" + name + "': factory returned null");
			}

			return new Registered<T>(instance);
		}

		private static Registered<ISeedCandidateProvider> BuildDns(
			DnsDiscoveryConfig dns,
			Registered<ISeedCandidateProvider> previous)
		{
			if (previous != null)
			{
				return previous;
			}

			return new Registered<ISeedCandidateProvider>(DiscoveryYaml.BuildDns(dns));
		}

		private static Registered<ISeedCandidateProvider> BuildAws(
			AwsDiscoveryConfig aws,
			Registered<ISeedCandidateProvider> previous)
		{
			if (previous != null)
			{
				return previous;
			}

			return new Registered<ISeedCandidateProvider>(DiscoveryYaml.BuildAws(aws));
		}

		private static void CheckRegistered(
			string path,
			string name,
			IDictionary<string, object> options,
			string[] yamlNames,
			IDictionary factories)
		{
			if (options != null && (factories == null || !factories.Contains(name)))
			{
				List<string> names = new List<string>(yamlNames);

				if (factories != null)
				{
					foreach (object key in factories.Keys)
					{
						names.Add(key.ToString());
					}
				}

				throw new AerospikeException("Invalid " + path + " value '" + name + "': valid values: " +
					string.Join(", ", names));
			}
		}

		private sealed class Registered<T>
		{
			internal readonly T instance;

			internal Registered(T instance)
			{
				this.instance = instance;
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
