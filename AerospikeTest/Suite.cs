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
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public static class Suite
	{
		[AssemblyInitialize()]
		public static void AssemblyInit(TestContext context)
		{
			SuiteHelpers.port = int.Parse(context.Properties["Port"].ToString());
			Console.WriteLine($"Port {SuiteHelpers.port}");
			Log.Info($"Port {SuiteHelpers.port}");
			SuiteHelpers.clusterName = context.Properties["ClusterName"].ToString();
			SuiteHelpers.user = context.Properties["User"].ToString();
			SuiteHelpers.password = context.Properties["Password"].ToString();
			SuiteHelpers.timeout = int.Parse(context.Properties["Timeout"].ToString());
			SuiteHelpers.ns = context.Properties["Namespace"].ToString();
			SuiteHelpers.set = context.Properties["Set"].ToString();
			SuiteHelpers.authMode = (AuthMode)Enum.Parse(typeof(AuthMode), context.Properties["AuthMode"].ToString(), true);
			SuiteHelpers.useServicesAlternate = bool.Parse(context.Properties["UseServicesAlternate"].ToString());

			bool tlsEnable = bool.Parse(context.Properties["TlsEnable"].ToString());

			if (tlsEnable)
			{
				SuiteHelpers.tlsName = context.Properties["TlsName"].ToString();
				SuiteHelpers.tlsPolicy = new TlsPolicy(
					context.Properties["TlsProtocols"].ToString(),
					context.Properties["TlsRevoke"].ToString(),
					context.Properties["TlsClientCertFile"].ToString(),
					bool.Parse(context.Properties["TlsLoginOnly"].ToString())
					);
			}

			var hostName = context.Properties["Host"].ToString();
			if (hostName == null || hostName == String.Empty)
			{
				SuiteHelpers.hosts = null;
			}
			else
			{
				SuiteHelpers.hosts = Host.ParseHosts(hostName, SuiteHelpers.tlsName, SuiteHelpers.port);
			}

			ConnectSync();
			ConnectAsync();
		}

		[AssemblyCleanup()]
		public static void AssemblyCleanup()
		{
			Close();
		}

		private static void ConnectSync()
		{
			ClientPolicy policy = new()
			{
				clusterName = SuiteHelpers.clusterName,
				tlsPolicy = SuiteHelpers.tlsPolicy,
				authMode = SuiteHelpers.authMode,
				timeout = SuiteHelpers.timeout,
				useServicesAlternate = SuiteHelpers.useServicesAlternate
			};

			if (SuiteHelpers.user != null && SuiteHelpers.user.Length > 0)
			{
				policy.user = SuiteHelpers.user;
				policy.password = SuiteHelpers.password;
			}

			SuiteHelpers.client = new AerospikeClient(policy, SuiteHelpers.hosts);

			try
			{
				SetServerSpecific();
			}
			catch
			{
				SuiteHelpers.client.Close();
				SuiteHelpers.client = null;
				throw;
			}
		}

		private static void ConnectAsync()
		{
			AsyncClientPolicy policy = new()
			{
				clusterName = SuiteHelpers.clusterName,
				tlsPolicy = SuiteHelpers.tlsPolicy,
				authMode = SuiteHelpers.authMode,
				asyncMaxCommands = 300,
				timeout = SuiteHelpers.timeout,
				useServicesAlternate = SuiteHelpers.useServicesAlternate
			};

			if (SuiteHelpers.user != null && SuiteHelpers.user.Length > 0)
			{
				policy.user = SuiteHelpers.user;
				policy.password = SuiteHelpers.password;
			}

			SuiteHelpers.asyncClient = new AsyncClient(policy, SuiteHelpers.hosts);
		}

		private static void SetServerSpecific()
		{
			Node node = SuiteHelpers.client.Nodes[0];
			string namespaceFilter = "namespace/" + SuiteHelpers.ns;
			Dictionary<string, string> map = Info.Request(null, node, "edition", namespaceFilter);

			string namespaceTokens = map[namespaceFilter] ?? throw new Exception(string.Format("Failed to get namespace info: host={0} namespace={1}", node, SuiteHelpers.ns));
			SuiteHelpers.singleBin = ParseBoolean(namespaceTokens, "single-bin");
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

			string value = namespaceTokens[begin..end];
			return Convert.ToBoolean(value);
		}

		public static string GetBinName(string name)
		{
			// Single bin servers don't need a bin name.
			return SuiteHelpers.singleBin ? "" : name;
		}

		public static void Close()
		{
			if (SuiteHelpers.client != null)
			{
				SuiteHelpers.client.Close();
				SuiteHelpers.client = null;
			}

			if (SuiteHelpers.asyncClient != null)
			{
				SuiteHelpers.asyncClient.Close();
				SuiteHelpers.asyncClient = null;
			}
		}
	}
}
