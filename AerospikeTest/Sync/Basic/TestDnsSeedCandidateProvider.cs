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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestDnsSeedCandidateProvider
	{
		private static readonly SeedCandidateContext Ctx = new SeedCandidateContext(null, [], 0);
		private static readonly Regex IpLiteral = new Regex(
			@"^(\d{1,3}(\.\d{1,3}){3}|[0-9a-fA-F:]*:[0-9a-fA-F:.]*)$", RegexOptions.Compiled);

		private StubDnsServer dns;

		[TestInitialize]
		public void SetUp()
		{
			dns = new StubDnsServer();
		}

		[TestCleanup]
		public void TearDown()
		{
			dns?.Dispose();
		}

		private DnsSeedCandidateProvider.Builder Builder(params string[] names)
		{
			return DnsSeedCandidateProvider.CreateBuilder().Names(names).Servers(dns.Address);
		}

		private static void AssertIpLiterals(List<Host> hosts)
		{
			foreach (Host host in hosts)
			{
				Assert.IsTrue(IpLiteral.IsMatch(host.name), "not an IP literal: " + host.name);
			}
		}

		[TestMethod]
		public void AAndAaaaRecords()
		{
			dns.ARecord("seeds.example.com", "10.0.0.9", "10.0.0.2").Aaaa("seeds.example.com", "2001:db8::1");
			DnsSeedCandidateProvider provider = Builder("seeds.example.com").Port(3100).Build();

			List<Host> hosts = provider.RefreshSeedCandidates(Ctx);

			Assert.AreEqual(3, hosts.Count);
			Assert.AreEqual("10.0.0.2", hosts[0].name);
			Assert.AreEqual(3100, hosts[0].port);
			Assert.AreEqual("10.0.0.9", hosts[1].name);
			Assert.AreEqual("2001:db8::1", hosts[2].name);
			CollectionAssert.AreEqual(hosts, provider.RefreshSeedCandidates(Ctx));
			AssertIpLiterals(hosts);
		}

		[TestMethod]
		public void DefaultPortIs3000()
		{
			dns.ARecord("seeds.example.com", "10.0.0.1");
			DnsSeedCandidateProvider provider = Builder("seeds.example.com").Build();

			List<Host> hosts = provider.RefreshSeedCandidates(Ctx);
			Assert.AreEqual(1, hosts.Count);
			Assert.AreEqual("10.0.0.1", hosts[0].name);
			Assert.AreEqual(3000, hosts[0].port);
			Assert.AreEqual(DnsSeedCandidateProvider.RecordType.A, provider.Type);
			Assert.AreEqual(DnsSeedCandidateProvider.DefaultPort, provider.Port);
			Assert.AreEqual(DnsSeedCandidateProvider.DefaultTimeout, provider.Timeout);
			Assert.IsNull(provider.TlsName);
		}

		[TestMethod]
		public void MultipleNamesInOrderAndDeduplicated()
		{
			dns.ARecord("b.example.com", "10.0.0.2", "10.0.0.3");
			dns.ARecord("a.example.com", "10.0.0.1", "10.0.0.2");
			DnsSeedCandidateProvider provider = Builder("b.example.com", "a.example.com.").Build();

			List<Host> hosts = provider.RefreshSeedCandidates(Ctx);

			Assert.AreEqual(3, hosts.Count);
			Assert.AreEqual("10.0.0.2", hosts[0].name);
			Assert.AreEqual("10.0.0.3", hosts[1].name);
			Assert.AreEqual("10.0.0.1", hosts[2].name);
		}

		[TestMethod]
		public void SrvTargetsFromFollowUpQueries()
		{
			dns.Srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
				.Srv("_aerospike._tcp.example.com", 10, 1, 3002, "node2.example.com")
				.ARecord("node1.example.com", "10.0.1.1")
				.Aaaa("node2.example.com", "2001:db8::2");
			DnsSeedCandidateProvider provider = Builder("_aerospike._tcp.example.com")
				.RecordType(DnsSeedCandidateProvider.RecordType.SRV).Build();

			List<Host> hosts = provider.RefreshSeedCandidates(Ctx);

			Assert.AreEqual(2, hosts.Count);
			Assert.AreEqual("10.0.1.1", hosts[0].name);
			Assert.AreEqual(3001, hosts[0].port);
			Assert.AreEqual("2001:db8::2", hosts[1].name);
			Assert.AreEqual(3002, hosts[1].port);
			AssertIpLiterals(hosts);
			Assert.IsTrue(dns.Queries.Contains("node1.example.com./" + StubDnsServer.A), string.Join(",", dns.Queries));
			Assert.IsTrue(dns.Queries.Contains("node2.example.com./" + StubDnsServer.AAAA), string.Join(",", dns.Queries));
		}

		[TestMethod]
		public void SrvOrderingByPriorityThenWeight()
		{
			dns.Srv("_aerospike._tcp.example.com", 20, 5, 3002, "node2.example.com")
				.Srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
				.Srv("_aerospike._tcp.example.com", 10, 9, 3003, "node3.example.com")
				.ARecord("node1.example.com", "10.0.1.1")
				.ARecord("node2.example.com", "10.0.1.2")
				.ARecord("node3.example.com", "10.0.1.3");
			DnsSeedCandidateProvider provider = Builder("_aerospike._tcp.example.com")
				.RecordType(DnsSeedCandidateProvider.RecordType.SRV).Port(4000).Build();

			List<Host> hosts = provider.RefreshSeedCandidates(Ctx);

			Assert.AreEqual(3, hosts.Count);
			Assert.AreEqual("10.0.1.3", hosts[0].name);
			Assert.AreEqual(3003, hosts[0].port);
			Assert.AreEqual("10.0.1.1", hosts[1].name);
			Assert.AreEqual(3001, hosts[1].port);
			Assert.AreEqual("10.0.1.2", hosts[2].name);
			Assert.AreEqual(3002, hosts[2].port);
		}

		[TestMethod]
		public void PartialFailureReturnsResolvedNames()
		{
			dns.ARecord("good.example.com", "10.0.0.1")
				.Rcode("bad.example.com", StubDnsServer.A, StubDnsServer.ServFail)
				.Rcode("bad.example.com", StubDnsServer.AAAA, StubDnsServer.ServFail);
			DnsSeedCandidateProvider provider = Builder("missing.example.com", "good.example.com", "bad.example.com").Build();

			List<Host> hosts = provider.RefreshSeedCandidates(Ctx);

			Assert.AreEqual(1, hosts.Count);
			Assert.AreEqual("10.0.0.1", hosts[0].name);
		}

		[TestMethod]
		public void EveryNameFailingThrowsNamingEach()
		{
			dns.Rcode("bad.example.com", StubDnsServer.A, StubDnsServer.ServFail)
				.Rcode("bad.example.com", StubDnsServer.AAAA, StubDnsServer.ServFail)
				.Srv("other.example.com", 1, 1, 1, "x.example.com");
			DnsSeedCandidateProvider provider = Builder("missing.example.com", "bad.example.com", "other.example.com").Build();

			AerospikeException ae = Assert.Throws<AerospikeException>(() => provider.RefreshSeedCandidates(Ctx));

			StringAssert.Contains(ae.BaseMessage, "missing.example.com: NXDOMAIN");
			StringAssert.Contains(ae.BaseMessage, "bad.example.com: SERVFAIL");
			StringAssert.Contains(ae.BaseMessage, "other.example.com: empty answer");
		}

		[TestMethod]
		public void UnansweredQueryReturnsWithinTimeout()
		{
			using (StubDnsServer silent = new StubDnsServer(true))
			{
				int timeout = 80;
				DnsSeedCandidateProvider provider = DnsSeedCandidateProvider.CreateBuilder()
					.Names("a.example.com", "b.example.com")
					.Timeout(timeout)
					.Servers(silent.Address)
					.Build();

				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(() => provider.RefreshSeedCandidates(Ctx));
				sw.Stop();

				Assert.IsTrue(sw.ElapsedMilliseconds <= timeout + 150, "elapsed " + sw.ElapsedMilliseconds);
				StringAssert.Contains(ae.BaseMessage, "a.example.com: timeout");
				StringAssert.Contains(ae.BaseMessage, "b.example.com: timeout");
			}
		}

		[TestMethod]
		public void TlsNameCarriedOnEveryHost()
		{
			dns.ARecord("seeds.example.com", "10.0.0.1", "10.0.0.2");
			DnsSeedCandidateProvider provider = Builder("seeds.example.com").TlsName("aerospike-tls").Build();

			List<Host> hosts = provider.RefreshSeedCandidates(Ctx);
			Assert.IsTrue(hosts.Count > 0);

			foreach (Host host in hosts)
			{
				Assert.AreEqual("aerospike-tls", host.tlsName);
			}

			Assert.AreEqual("aerospike-tls", provider.TlsName);
		}

		[TestMethod]
		public void NeedsPeriodicRefreshAndSupportsDeadline()
		{
			DnsSeedCandidateProvider provider = Builder("seeds.example.com").Build();

			Assert.IsTrue(provider.NeedsPeriodicRefresh());
			Assert.IsTrue(provider.SupportsDeadline());
		}

		[TestMethod]
		public void DnsTimeoutMustNotExceedTendDeadline()
		{
			Host[] seeds = [new Host("127.0.0.1", 3000)];
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 30000,
				discoveryTendRefreshDeadlineMs = 50
			};
			DnsSeedCandidateProvider provider = Builder("seeds.example.com").Timeout(80).Build();

			AerospikeException ex = Assert.Throws<AerospikeException>(() =>
				new SeedCandidateManager(provider, policy, seeds, _ => { }, Log.Context.Empty));

			StringAssert.Contains(ex.Message, "DNS seed provider timeout");
		}

		[TestMethod]
		public void ProviderNameIsDns()
		{
			Assert.AreEqual("dns", SeedCandidateManager.ProviderName(Builder("seeds.example.com").Build()));
		}

		[TestMethod]
		public void InvalidConfigRejectedAtConstruction()
		{
			AssertRejected(b => b.Names(), "at least one name is required");
			AssertRejected(b => b.Names("a.example.com", ""), "Invalid DNS seed provider name");
			AssertRejected(b => b.Names("a.example.com", "A.example.com"), "duplicate");
			AssertRejected(b => b.Port(0), "Invalid DNS seed provider port: 0");
			AssertRejected(b => b.Timeout(0), "Invalid DNS seed provider timeout: 0");
			AssertRejected(b => b.TlsName(""), "tlsName: empty");
			AssertRejected(b => b.Servers("dns.example.com"), "expected an IP address");
			AssertRejected(b => b.Servers("10.0.0.1:70000"), "invalid port");

			DnsSeedCandidateProvider provider = DnsSeedCandidateProvider.CreateBuilder()
				.Names("a.example.com")
				.Servers("10.0.0.1", "10.0.0.2:5353", "2001:db8::1", "[2001:db8::2]:5353", "[::1]")
				.Build();

			Assert.AreEqual(5, provider.Servers.Count);
		}

		private static void AssertRejected(Action<DnsSeedCandidateProvider.Builder> config, string message)
		{
			DnsSeedCandidateProvider.Builder builder = DnsSeedCandidateProvider.CreateBuilder()
				.Names("seeds.example.com")
				.Servers("127.0.0.1");
			config(builder);

			AerospikeException ae = Assert.Throws<AerospikeException>(() => builder.Build());
			StringAssert.Contains(ae.BaseMessage, message);
		}
	}
}
