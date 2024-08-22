/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using Aerospike.Client;
using Aerospike.Client.Proxy;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class Args
	{
		public static Args Instance = new Args();

		public IAerospikeClient client;
		public AerospikeClient nativeClient;
		public IAsyncClient asyncClient;
		public AsyncClient nativeAsync;
		public AsyncClientProxy asyncProxy;
		public AerospikeClientProxy proxyClient;
		public Host[] hosts;
		public Host proxyHost;
		public int port;
		public int proxyPort;
		public bool testProxy;
		public string user;
		public string password;
		public int timeout;
		public string clusterName;
		public string ns;
		public string set;
		public bool useServicesAlternate;
		public string tlsName;
		public string proxyTlsName;
		public TlsPolicy tlsPolicy;
		public TlsPolicy proxyTlsPolicy;
		public AuthMode authMode;
		public bool singleBin;
		public bool enterprise;
		public int proxyTotalTimeout;
		public int proxySocketTimeout;

		public Args()
		{
			Log.Disable();

			var builder = new ConfigurationBuilder().AddJsonFile("settings.json", optional: true, reloadOnChange: true);
			IConfigurationRoot section = builder.Build();

			port = int.Parse(section.GetSection("Port").Value);
			proxyPort = int.Parse(section.GetSection("ProxyPort").Value);
			testProxy = bool.Parse(section.GetSection("TestProxy").Value);
			clusterName = section.GetSection("ClusterName").Value;
			user = section.GetSection("User").Value;
			password = section.GetSection("Password").Value;
			timeout = int.Parse(section.GetSection("Timeout").Value);
			ns = section.GetSection("Namespace").Value;
			set = section.GetSection("Set").Value;
			authMode = (AuthMode)Enum.Parse(typeof(AuthMode), section.GetSection("AuthMode").Value, true);
			useServicesAlternate = bool.Parse(section.GetSection("UseServicesAlternate").Value);

			bool tlsEnable = bool.Parse(section.GetSection("TlsEnable").Value);

			if (tlsEnable)
			{
				tlsName = section.GetSection("TlsName").Value;
				tlsPolicy = new TlsPolicy(
					section.GetSection("TlsProtocols").Value,
					section.GetSection("TlsRevoke").Value,
					section.GetSection("TlsClientCertFile").Value,
					bool.Parse(section.GetSection("TlsLoginOnly").Value)
					);
			}

			bool tlsEnableProxy = bool.Parse(section.GetSection("ProxyTlsEnable").Value);

			if (tlsEnableProxy)
			{
				proxyTlsName = section.GetSection("ProxyTlsName").Value;
				proxyTlsPolicy = new TlsPolicy(
					section.GetSection("ProxyTlsProtocols").Value,
					section.GetSection("ProxyTlsRevoke").Value,
					section.GetSection("ProxyTlsClientCertFile").Value,
					bool.Parse(section.GetSection("ProxyTlsLoginOnly").Value)
					);
			}

			var hostName = section.GetSection("Host").Value;
			if (hostName == null || hostName == String.Empty)
			{
				hosts = null;
			}
			else
			{
				hosts = Host.ParseHosts(hostName, tlsName, port);
			}
			
			proxyHost = Host.ParseHosts(section.GetSection("ProxyHost").Value, proxyTlsName, proxyPort)[0];
		}

		public void Connect()
		{
			if (testProxy)
			{
				ConnectProxy();
			}
			else
			{
				ConnectSync();
				ConnectAsync();
			}
		}

		private void ConnectSync()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.clusterName = clusterName;
			policy.tlsPolicy = tlsPolicy;
			policy.authMode = authMode;
			policy.timeout = timeout;
			policy.useServicesAlternate = useServicesAlternate;

			if (user != null && user.Length > 0)
			{
				policy.user = user;
				policy.password = password;
			}

			nativeClient = new AerospikeClient(policy, hosts);

			nativeClient.readPolicyDefault.totalTimeout = timeout;
			nativeClient.WritePolicyDefault.totalTimeout = timeout;
			nativeClient.ScanPolicyDefault.totalTimeout = timeout;
			nativeClient.QueryPolicyDefault.totalTimeout = timeout;
			nativeClient.BatchPolicyDefault.totalTimeout = timeout;
			nativeClient.BatchParentPolicyWriteDefault.totalTimeout = timeout;
			nativeClient.InfoPolicyDefault.timeout = timeout;
			client = nativeClient;

			//Example of how to enable metrics
			//client.EnableMetrics(new MetricsPolicy());

			try
			{
				SetServerSpecific();
			}
			catch
			{
				client.Close();
				client = null;
				throw;
			}
		}

		private void ConnectProxy()
		{
			ClientPolicy policy = new ClientPolicy();
			ClientPolicy proxyPolicy = new ClientPolicy();
			AsyncClientPolicy asyncPolicy = new AsyncClientPolicy();
			AsyncClientPolicy proxyAsyncPolicy = new AsyncClientPolicy();
			policy.clusterName = clusterName;
			proxyPolicy.clusterName = clusterName;
			asyncPolicy.clusterName = clusterName;
			proxyAsyncPolicy.clusterName = clusterName;
			policy.tlsPolicy = tlsPolicy;
			proxyPolicy.tlsPolicy = proxyTlsPolicy;
			asyncPolicy.tlsPolicy = tlsPolicy;
			proxyAsyncPolicy.tlsPolicy = proxyTlsPolicy;
			policy.authMode = authMode;
			proxyPolicy.authMode = authMode;
			asyncPolicy.authMode = authMode;
			proxyAsyncPolicy.authMode = authMode;
			proxyPolicy.minConnsPerNode = 100;
			proxyAsyncPolicy.minConnsPerNode = 100;
			proxyPolicy.maxConnsPerNode = 100;
			proxyAsyncPolicy.maxConnsPerNode = 100;
			proxyPolicy.timeout = timeout;
			proxyAsyncPolicy.timeout = timeout;
			policy.useServicesAlternate = useServicesAlternate;
			proxyPolicy.useServicesAlternate = useServicesAlternate;
			asyncPolicy.useServicesAlternate = useServicesAlternate;
			proxyAsyncPolicy.useServicesAlternate = useServicesAlternate;


			if (user != null && user.Length > 0)
			{
				policy.user = user;
				policy.password = password;
				proxyPolicy.user = user;
				proxyPolicy.password = password;
				asyncPolicy.user = user;
				asyncPolicy.password = password;
				proxyAsyncPolicy.user = user;
				proxyAsyncPolicy.password = password;
			}

			asyncPolicy.asyncMaxCommands = 300;
			proxyAsyncPolicy.asyncMaxCommands = 300;

			proxyClient = new AerospikeClientProxy(proxyPolicy, proxyHost);
			if (hosts != null)
			{
				nativeClient = new AerospikeClient(policy, hosts);
				nativeAsync = new AsyncClient(asyncPolicy, hosts);
			}
			else
			{
				nativeClient = null;
				nativeAsync = null;
			}
			
			asyncProxy = new AsyncClientProxy(proxyAsyncPolicy, proxyHost);
			asyncClient = asyncProxy;

			proxyTotalTimeout = timeout;
			proxySocketTimeout = 5000;

			proxyClient.readPolicyDefault.totalTimeout = proxyTotalTimeout;
			proxyClient.readPolicyDefault.socketTimeout = proxySocketTimeout;
			proxyClient.WritePolicyDefault.totalTimeout = proxyTotalTimeout;
			proxyClient.WritePolicyDefault.socketTimeout = proxySocketTimeout;
			proxyClient.ScanPolicyDefault.totalTimeout = proxyTotalTimeout;
			proxyClient.ScanPolicyDefault.socketTimeout = proxySocketTimeout;
			proxyClient.QueryPolicyDefault.totalTimeout = proxyTotalTimeout;
			proxyClient.QueryPolicyDefault.socketTimeout = proxySocketTimeout;
			proxyClient.BatchPolicyDefault.totalTimeout = proxyTotalTimeout;
			proxyClient.BatchPolicyDefault.socketTimeout = proxySocketTimeout;
			proxyClient.BatchParentPolicyWriteDefault.totalTimeout = proxyTotalTimeout;
			proxyClient.BatchParentPolicyWriteDefault.socketTimeout = proxySocketTimeout;
			proxyClient.InfoPolicyDefault.timeout = proxyTotalTimeout;

			asyncProxy.ReadPolicyDefault = proxyClient.ReadPolicyDefault;
			asyncProxy.WritePolicyDefault = proxyClient.WritePolicyDefault;
			asyncProxy.ScanPolicyDefault = proxyClient.ScanPolicyDefault;
			asyncProxy.QueryPolicyDefault = proxyClient.QueryPolicyDefault;
			asyncProxy.BatchPolicyDefault = proxyClient.BatchPolicyDefault;
			asyncProxy.BatchParentPolicyWriteDefault = proxyClient.BatchParentPolicyWriteDefault;
			asyncProxy.InfoPolicyDefault = proxyClient.InfoPolicyDefault;

			if (nativeClient != null)
			{
				nativeClient.ReadPolicyDefault = proxyClient.ReadPolicyDefault;
				nativeClient.WritePolicyDefault = proxyClient.WritePolicyDefault;
				nativeClient.ScanPolicyDefault = proxyClient.ScanPolicyDefault;
				nativeClient.QueryPolicyDefault = proxyClient.QueryPolicyDefault;
				nativeClient.BatchPolicyDefault = proxyClient.BatchPolicyDefault;
				nativeClient.BatchParentPolicyWriteDefault = proxyClient.BatchParentPolicyWriteDefault;
				nativeClient.InfoPolicyDefault = proxyClient.InfoPolicyDefault;

				asyncClient.ReadPolicyDefault = proxyClient.ReadPolicyDefault;
				asyncClient.WritePolicyDefault = proxyClient.WritePolicyDefault;
				asyncClient.ScanPolicyDefault = proxyClient.ScanPolicyDefault;
				asyncClient.QueryPolicyDefault = proxyClient.QueryPolicyDefault;
				asyncClient.BatchPolicyDefault = proxyClient.BatchPolicyDefault;
				asyncClient.BatchParentPolicyWriteDefault = proxyClient.BatchParentPolicyWriteDefault;
				asyncClient.InfoPolicyDefault = proxyClient.InfoPolicyDefault;
			}

			client = proxyClient;

			try
			{
				if (nativeClient != null)
				{
					SetServerSpecific();
				}
			}
			catch
			{
				client.Close();
				client = null;
				throw;
			}
		}

		private void ConnectAsync()
		{
			AsyncClientPolicy policy = new AsyncClientPolicy();
			policy.clusterName = clusterName;
			policy.tlsPolicy = tlsPolicy;
			policy.authMode = authMode;
			policy.asyncMaxCommands = 300;
			policy.timeout = timeout;
			policy.useServicesAlternate = useServicesAlternate;

			if (user != null && user.Length > 0)
			{
				policy.user = user;
				policy.password = password;
			}

			nativeAsync = new AsyncClient(policy, hosts);

			nativeAsync.readPolicyDefault.totalTimeout = timeout;
			nativeAsync.WritePolicyDefault.totalTimeout = timeout;
			nativeAsync.ScanPolicyDefault.totalTimeout = timeout;
			nativeAsync.QueryPolicyDefault.totalTimeout = timeout;
			nativeAsync.BatchPolicyDefault.totalTimeout = timeout;
			nativeAsync.BatchParentPolicyWriteDefault.totalTimeout = timeout;
			nativeAsync.InfoPolicyDefault.timeout = timeout;

			asyncClient = nativeAsync;

			// Example of how to enable metrics
			//asyncClient.EnableMetrics(new MetricsPolicy());
		}

		private void SetServerSpecific()
		{
			Node node = nativeClient.Nodes[0];
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> map = Info.Request(null, node, "edition", namespaceFilter);

			string edition = map["edition"];
			enterprise = edition.Equals("Aerospike Enterprise Edition");

			string namespaceTokens = map[namespaceFilter];

			if (namespaceTokens == null)
			{
				throw new Exception(string.Format("Failed to get namespace info: host={0} namespace={1}", node, ns));
			}

			singleBin = ParseBoolean(namespaceTokens, "single-bin");
		}

		private static bool ParseBoolean(String namespaceTokens, String name)
		{
			string search = name + '=';
			int begin = namespaceTokens.IndexOf(search);

			if (begin < 0)
			{
				return false;
			}

			begin += search.Length;
			int end = namespaceTokens.IndexOf(';', begin);

			if (end < 0)
			{
				end = namespaceTokens.Length;
			}

			string value = namespaceTokens.Substring(begin, end - begin);
			return Convert.ToBoolean(value);
		}

		public string GetBinName(string name)
		{
			// Single bin servers don't need a bin name.
			return singleBin ? "" : name;
		}

		public void Close()
		{
			if (client != null)
			{
				client.Close();
				client = null;
			}

			if (asyncClient != null)
			{
				asyncClient.Close();
				asyncClient = null;
			}
		}
	}
}
