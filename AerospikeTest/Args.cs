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
using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class Args
	{
		public static Args Instance = new Args();

		public AerospikeClient client;
		public AsyncClient asyncClient;
		public string host;
		public int port;
		public string user;
		public string password;
		public string ns;
		public string set;
		public bool prompt;
		public bool hasUdf;
		public bool singleBin;
		public bool hasLargeDataTypes;

		public Args()
		{
			host = Properties.Settings.Default.Host;
			port = Properties.Settings.Default.Port;
			user = Properties.Settings.Default.User;
			ns = Properties.Settings.Default.Namespace;
			set = Properties.Settings.Default.Set;
			prompt = Properties.Settings.Default.Prompt;
		}

		public void Save()
		{
			// This doesn't actually work because the test project is a library
			// (not an application) and app.config is only saved for applications.
			Properties.Settings.Default.Host = host;
			Properties.Settings.Default.Port = port;
			Properties.Settings.Default.User = user;
			Properties.Settings.Default.Namespace = ns;
			Properties.Settings.Default.Set = set;
			Properties.Settings.Default.Prompt = prompt;

			Properties.Settings.Default.Save();
		}

		public void Connect()
		{
			ConnectSync();
			ConnectAsync();
		}

		private void ConnectSync()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.failIfNotConnected = true;

			if (!user.Equals(""))
			{
				policy.user = user;
				policy.password = password;
			}

			client = new AerospikeClient(policy, host, port);

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
			policy.failIfNotConnected = true;

			if (!user.Equals(""))
			{
				policy.user = user;
				policy.password = password;
			}

			asyncClient = new AsyncClient(policy, host, port);
		}

		private void SetServerSpecific()
		{
			Node node = client.Nodes[0];
			string featuresFilter = "features";
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> tokens = Info.Request(null, node, featuresFilter, namespaceFilter);

			string features = tokens[featuresFilter];
			hasUdf = false;

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
