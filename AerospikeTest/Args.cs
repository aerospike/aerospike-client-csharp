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
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class Args
	{
		public static Args Instance = new Args();

		public IAerospikeClient client;
		public IAerospikeClientNew asyncAwaitClient;
		public IAsyncClient asyncClient;
		public Host[] hosts;
		public int port;
		public bool testAsyncAwait;
		public string user;
		public string password;
		public int timeout;
		public string clusterName;
		public string ns;
		public string set;
		public bool useServicesAlternate;
		public string tlsName;
		public TlsPolicy tlsPolicy;
		public AuthMode authMode;
		public bool singleBin;
		public bool enterprise;

		public Args()
		{
			Log.Disable();

			var builder = new ConfigurationBuilder().AddJsonFile("settings.json", optional: true, reloadOnChange: true);
			IConfigurationRoot section = builder.Build();

			port = int.Parse(section.GetSection("Port").Value);
			testAsyncAwait = bool.Parse(section.GetSection("TestAsyncAwait").Value);
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

			var hostName = section.GetSection("Host").Value;
			if (hostName == null || hostName == String.Empty)
			{
				hosts = null;
			}
			else
			{
				hosts = Host.ParseHosts(hostName, tlsName, port);
			}
		}

		public void Connect()
		{
			if (testAsyncAwait)
			{
				ConnectAsyncAwait();
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

			client = new AerospikeClient(policy, hosts);

			asyncAwaitClient = new AerospikeClientNew(policy, hosts);

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

		private void ConnectAsyncAwait()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.clusterName = clusterName;
			policy.tlsPolicy = tlsPolicy;
			policy.authMode = authMode;
			policy.timeout = timeout;

			if (user != null && user.Length > 0)
			{
				policy.user = user;
				policy.password = password;
			}

			asyncAwaitClient = new AerospikeClientNew(policy, hosts);

			asyncAwaitClient.ReadPolicyDefault.totalTimeout = timeout;
			asyncAwaitClient.WritePolicyDefault.totalTimeout = timeout;
			asyncAwaitClient.ScanPolicyDefault.totalTimeout = timeout;
			asyncAwaitClient.QueryPolicyDefault.totalTimeout = timeout;
			asyncAwaitClient.BatchPolicyDefault.totalTimeout = timeout;
			asyncAwaitClient.BatchParentPolicyWriteDefault.totalTimeout = timeout;
			asyncAwaitClient.InfoPolicyDefault.timeout = timeout;

			client = new AerospikeClient(policy, hosts);

			//Example of how to enable metrics
			//client.EnableMetrics(new MetricsPolicy());

			try
			{
				SetServerSpecific();
			}
			catch
			{
				asyncAwaitClient.Close();
				asyncAwaitClient = null;
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

			asyncClient = new AsyncClient(policy, hosts);

			// Example of how to enable metrics
			//asyncClient.EnableMetrics(new MetricsPolicy());
		}

		private void SetServerSpecific()
		{
			Node node = client.Nodes[0];
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
