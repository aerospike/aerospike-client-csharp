﻿/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using Microsoft.Extensions.Configuration;
using Aerospike.Client;

namespace Aerospike.Test
{
	public class Args
	{
		public static Args Instance = new Args();

		public Client.AerospikeClient client;
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
		public bool hasUdf;
		public bool hasMap;
		public bool singleBin;
		public bool hasLargeDataTypes;

		public Args()
		{
			var builder = new ConfigurationBuilder()
				.AddJsonFile("settings.json", optional: true, reloadOnChange: true);
			IConfigurationRoot section = builder.Build();

			port = int.Parse(section.GetSection("Port").Value);
			clusterName = section.GetSection("ClusterName").Value;
			user = section.GetSection("User").Value;
			password = section.GetSection("Password").Value;
			ns = section.GetSection("Namespace").Value;
			set = section.GetSection("Set").Value;

			bool tlsEnable = bool.Parse(section.GetSection("TlsEnable").Value);

			if (tlsEnable)
			{
				tlsName = section.GetSection("TlsName").Value;
				tlsPolicy = new TlsPolicy(
					section.GetSection("TlsProtocols").Value,
					section.GetSection("TlsRevoke").Value,
					section.GetSection("TlsClientCertFile").Value
					);
			}

			hosts = Host.ParseHosts(section.GetSection("Host").Value, tlsName, port);
		}

		public void Connect()
		{
			ConnectSync();

			// SSL only works with synchronous commands.
			if (tlsPolicy == null)
			{
				ConnectAsync();
			}
		}

		private void ConnectSync()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.clusterName = clusterName;
			policy.tlsPolicy = tlsPolicy;

			if (!user.Equals(""))
			{
				policy.user = user;
				policy.password = password;
			}

			client = new Client.AerospikeClient(policy, hosts);

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
			policy.asyncMaxCommands = 300;

			if (!user.Equals(""))
			{
				policy.user = user;
				policy.password = password;
			}

			asyncClient = new AsyncClient(policy, hosts);
		}

		private void SetServerSpecific()
		{
			Node node = client.Nodes[0];
			string featuresFilter = "features";
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> tokens = Info.Request(null, node, featuresFilter, namespaceFilter);

			string features = tokens[featuresFilter];
			hasUdf = false;
			hasMap = false;

			if (features != null)
			{
				string[] list = features.Split(';');

				foreach (string s in list)
				{
					if (s.Equals("udf"))
					{
						hasUdf = true;
						break;
					}
					else if (s.Equals("cdt-map"))
					{
						hasMap = true;
						break;
					}
				}
			}

			string namespaceTokens = tokens[namespaceFilter];

			if (namespaceTokens == null)
			{
				throw new Exception(string.Format("Failed to get namespace info: host={0} namespace={1}", node, ns));
			}

			singleBin = ParseBoolean(namespaceTokens, "single-bin");
			hasLargeDataTypes = ParseBoolean(namespaceTokens, "ldt-enabled");
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

		public bool ValidateLDT()
		{
			return hasLargeDataTypes;
		}

		public bool ValidateMap() 
		{
			return hasMap;
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
