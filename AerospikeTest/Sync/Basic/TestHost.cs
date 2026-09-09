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
	public class TestHost
	{
		[TestMethod]
		public void HostEqualsIgnoresTlsName()
		{
			Host plain = new("127.0.0.1", 3000);
			Host withTls = new("127.0.0.1", "localhost", 3000);
			Host differentPort = new("127.0.0.1", 3001);

			Assert.AreEqual(plain, withTls);
			Assert.AreEqual(plain.GetHashCode(), withTls.GetHashCode());
			Assert.AreNotEqual(plain, differentPort);
			Assert.AreEqual("127.0.0.1 3000", plain.ToString());
		}

		[TestMethod]
		public void ParseHostsSupportsTlsNameAndIpv6()
		{
			Host[] hosts = Host.ParseHosts("db.example.com:db.example.com:4000,[::1]:tls.example.com:5000", null, 3000);

			Assert.AreEqual(2, hosts.Length);
			Assert.AreEqual("db.example.com", hosts[0].name);
			Assert.AreEqual("db.example.com", hosts[0].tlsName);
			Assert.AreEqual(4000, hosts[0].port);
			Assert.AreEqual("::1", hosts[1].name);
			Assert.AreEqual("tls.example.com", hosts[1].tlsName);
			Assert.AreEqual(5000, hosts[1].port);
		}

		[TestMethod]
		public void ParseServiceHostsRequiresExplicitPorts()
		{
			List<Host> hosts = Host.ParseServiceHosts("127.0.0.1:3000,db.example.com:4000");

			Assert.AreEqual(2, hosts.Count);
			Assert.AreEqual("127.0.0.1", hosts[0].name);
			Assert.AreEqual(3000, hosts[0].port);
			Assert.AreEqual("db.example.com", hosts[1].name);
			Assert.AreEqual(4000, hosts[1].port);
		}
	}
}
