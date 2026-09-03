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
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestHostParse
	{
		private const int DefaultPort = 3000;
		private const string DefaultTls = "default-tls";

		[TestMethod]
		public void ParseHostsSingleHostWithPort()
		{
			Host[] hosts = Host.ParseHosts("db.example.com:3100", DefaultTls, DefaultPort);

			Assert.AreEqual(1, hosts.Length);
			Assert.AreEqual("db.example.com", hosts[0].name);
			Assert.AreEqual(3100, hosts[0].port);
			Assert.AreEqual(DefaultTls, hosts[0].tlsName);
		}

		[TestMethod]
		public void ParseHostsMultipleHosts()
		{
			Host[] hosts = Host.ParseHosts("host1:3001,host2:3002", DefaultTls, DefaultPort);

			Assert.AreEqual(2, hosts.Length);
			Assert.AreEqual("host1", hosts[0].name);
			Assert.AreEqual(3001, hosts[0].port);
			Assert.AreEqual("host2", hosts[1].name);
			Assert.AreEqual(3002, hosts[1].port);
		}

		[TestMethod]
		public void ParseHostsTlsNameAndPort()
		{
			Host[] hosts = Host.ParseHosts("10.0.0.1:cluster-tls:4000", DefaultTls, DefaultPort);

			Assert.AreEqual(1, hosts.Length);
			Assert.AreEqual("10.0.0.1", hosts[0].name);
			Assert.AreEqual("cluster-tls", hosts[0].tlsName);
			Assert.AreEqual(4000, hosts[0].port);
		}

		[TestMethod]
		public void ParseHostsIpv6WithPort()
		{
			Host[] hosts = Host.ParseHosts("[::1]:3100", DefaultTls, DefaultPort);

			Assert.AreEqual(1, hosts.Length);
			Assert.AreEqual("::1", hosts[0].name);
			Assert.AreEqual(3100, hosts[0].port);
		}

		[TestMethod]
		public void ParseHostsUsesDefaultPort()
		{
			Host[] hosts = Host.ParseHosts("localhost", DefaultTls, 3010);

			Assert.AreEqual(1, hosts.Length);
			Assert.AreEqual("localhost", hosts[0].name);
			Assert.AreEqual(3010, hosts[0].port);
		}

		[TestMethod]
		public void ParseServiceHostsMultiplePorts()
		{
			List<Host> hosts = Host.ParseServiceHosts("node1:3001,node2:3002");

			Assert.AreEqual(2, hosts.Count);
			Assert.AreEqual("node1", hosts[0].name);
			Assert.AreEqual(3001, hosts[0].port);
			Assert.AreEqual("node2", hosts[1].name);
			Assert.AreEqual(3002, hosts[1].port);
		}

		[TestMethod]
		public void ParseHostsInvalidStringThrows()
		{
			try
			{
				Host.ParseHosts("[unclosed", DefaultTls, DefaultPort);
				Assert.Fail("Expected AerospikeException for invalid host string");
			}
			catch (AerospikeException)
			{
			}
		}
	}
}
