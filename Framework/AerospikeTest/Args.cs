﻿/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

#if !NETFRAMEWORK
using Microsoft.Extensions.Configuration;
#endif

namespace Aerospike.Test
{
	[TestClass]
	public class Args
	{
		public static Args Instance = new Args();

		public AerospikeClient client;
		public AsyncClient asyncClient;
		public Host[] hosts;
		public int port;
		public string user;
		public string password;
		public string clusterName;
		public string ns;
		public string set;
		public string tlsName;
		public TlsPolicy tlsPolicy;
		public AuthMode authMode;
		public bool singleBin;
		public bool enterprise;

		public Args()
		{
			Log.Disable();

#if NETFRAMEWORK
            port = Properties.Settings.Default.Port;
			clusterName = Properties.Settings.Default.ClusterName.Trim();
			user = Properties.Settings.Default.User.Trim();
			password = Properties.Settings.Default.Password.Trim();
			ns = Properties.Settings.Default.Namespace.Trim();
			set = Properties.Settings.Default.Set.Trim();
			authMode = (AuthMode)Enum.Parse(typeof(AuthMode), Properties.Settings.Default.AuthMode.Trim(), true);

			if (Properties.Settings.Default.TlsEnable)
			{
				tlsName = Properties.Settings.Default.TlsName.Trim();
				tlsPolicy = new TlsPolicy(
					Properties.Settings.Default.TlsProtocols,
					Properties.Settings.Default.TlsRevoke,
					Properties.Settings.Default.TlsClientCertFile,
					Properties.Settings.Default.TlsLoginOnly
					);
			}

			hosts = Host.ParseHosts(Properties.Settings.Default.Host, tlsName, port);
#else
            var builder = new ConfigurationBuilder()
                .AddJsonFile("settings.json", optional: true, reloadOnChange: true);
            IConfigurationRoot section = builder.Build();

            port = int.Parse(section.GetSection("Port").Value);
            clusterName = section.GetSection("ClusterName").Value;
            user = section.GetSection("User").Value;
            password = section.GetSection("Password").Value;
            ns = section.GetSection("Namespace").Value;
            set = section.GetSection("Set").Value;
            authMode = (AuthMode)Enum.Parse(typeof(AuthMode), section.GetSection("AuthMode").Value, true);

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

            hosts = Host.ParseHosts(section.GetSection("Host").Value, tlsName, port);
#endif
        }

        public void Connect()
		{
			ConnectSync();
			ConnectAsync();
		}

		private void ConnectSync()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.clusterName = clusterName;
			policy.tlsPolicy = tlsPolicy;
			policy.authMode = authMode;

			if (user != null && user.Length > 0)
			{
				policy.user = user;
				policy.password = password;
			}

			client = new AerospikeClient(policy, hosts);

			try
			{
				SetServerSpecific();
			}
			catch (Exception e)
			{
				client.Close();
				client = null;
				throw e;
			}
		}

		private void ConnectAsync()
		{
			AsyncClientPolicy policy = new AsyncClientPolicy();
			policy.clusterName = clusterName;
			policy.tlsPolicy = tlsPolicy;
			policy.authMode = authMode;
			policy.asyncMaxCommands = 300;

			if (user != null && user.Length > 0)
			{
				policy.user = user;
				policy.password = password;
			}

			asyncClient = new AsyncClient(policy, hosts);
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
