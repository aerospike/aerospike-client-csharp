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
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Server-free tests for pluggable discovery SPI, merge policy, translators, and manager.
	/// </summary>
	[TestClass]
	public class TestDiscovery
	{
		[TestMethod]
		public void StaticSeedProviderReturnsConfiguredHosts()
		{
			Host[] seeds = [new Host("a", 3000), new Host("b", 3001)];
			StaticSeedCandidateProvider provider = new StaticSeedCandidateProvider(seeds);
			SeedCandidateContext ctx = new SeedCandidateContext("cluster", seeds, 0);

			List<Host> result = provider.RefreshSeedCandidates(ctx);

			Assert.AreEqual(2, result.Count);
			Assert.AreEqual("a", result[0].name);
			Assert.AreEqual(3000, result[0].port);
			Assert.IsFalse(provider.NeedsPeriodicRefresh());
			Assert.IsTrue(provider.SupportsDeadline());
		}

		[TestMethod]
		public void StaticMapTranslatorMapsHostKeepsPortAndTlsName()
		{
			Dictionary<string, string> map = new Dictionary<string, string>
			{
				["10.0.0.1"] = "192.168.1.10"
			};
			StaticMapAddressTranslator translator = new StaticMapAddressTranslator(map);
			Endpoint advertised = new Endpoint("BB1", "tls.example", "10.0.0.1", 3000, Endpoint.SourceList.STANDARD);

			Host mapped = translator.Translate(advertised);

			Assert.IsNotNull(mapped);
			Assert.AreEqual("192.168.1.10", mapped.name);
			Assert.AreEqual(3000, mapped.port);
			Assert.AreEqual("tls.example", mapped.tlsName);
		}

		[TestMethod]
		public void StaticMapTranslatorMissWhenUnmapped()
		{
			StaticMapAddressTranslator translator = new StaticMapAddressTranslator(new Dictionary<string, string>());
			Endpoint advertised = new Endpoint("BB1", null, "10.0.0.1", 3000, Endpoint.SourceList.STANDARD);

			Assert.IsNull(translator.Translate(advertised));
		}

		[TestMethod]
		public void PassThroughNeverMisses()
		{
			Endpoint advertised = new Endpoint("BB1", "tls", "host", 4000, Endpoint.SourceList.ALTERNATE);
			Host mapped = PassThroughAddressTranslator.Instance.Translate(advertised);

			Assert.AreEqual("host", mapped.name);
			Assert.AreEqual(4000, mapped.port);
			Assert.AreEqual("tls", mapped.tlsName);
		}

		[TestMethod]
		public void PortMapHostPortKeyReplacesHostAndPortKeepsTls()
		{
			Dictionary<string, string> map = new Dictionary<string, string>
			{
				["10.0.0.1:3000"] = "lb.example.com:30001",
				["10.0.0.2:3000"] = "lb.example.com:30002"
			};
			PortMapAddressTranslator translator = new PortMapAddressTranslator(map);
			Endpoint advertised = new Endpoint("BB9020011AC4202", "db1", "10.0.0.1", 3000, Endpoint.SourceList.STANDARD);

			Host mapped = translator.Translate(advertised);

			Assert.IsNotNull(mapped);
			Assert.AreEqual("lb.example.com", mapped.name);
			Assert.AreEqual(30001, mapped.port);
			Assert.AreEqual("db1", mapped.tlsName);
			Assert.AreEqual("port-map", SeedCandidateManager.TranslatorName(translator));
		}

		[TestMethod]
		public void PortMapNodeNameKeyHit()
		{
			string nodeA = "BB9020011AC4202";
			string nodeB = "BB9030011AC4202";
			Dictionary<string, string> map = new Dictionary<string, string>
			{
				[nodeA] = "lb.example.com:30001",
				[nodeB] = "lb.example.com:30002"
			};
			PortMapAddressTranslator translator = new PortMapAddressTranslator(map);

			Host mapped = translator.Translate(new Endpoint(nodeB, "db2", "10.0.0.2", 3000, Endpoint.SourceList.STANDARD));

			Assert.AreEqual("lb.example.com", mapped.name);
			Assert.AreEqual(30002, mapped.port);
			Assert.AreEqual("db2", mapped.tlsName);
		}

		[TestMethod]
		public void PortMapHostPortTakesPrecedenceOverNodeName()
		{
			string nodeA = "BB9020011AC4202";
			Dictionary<string, string> map = new Dictionary<string, string>
			{
				["10.0.0.1:3000"] = "lb.example.com:30001",
				[nodeA] = "lb.example.com:30002"
			};
			PortMapAddressTranslator translator = new PortMapAddressTranslator(map);

			Host byEndpoint = translator.Translate(new Endpoint(nodeA, "db1", "10.0.0.1", 3000, Endpoint.SourceList.STANDARD));
			Host byNode = translator.Translate(new Endpoint(nodeA, "db1", "10.0.0.1", 4000, Endpoint.SourceList.STANDARD));

			Assert.AreEqual(30001, byEndpoint.port);
			Assert.AreEqual(30002, byNode.port);
		}

		[TestMethod]
		public void PortMapIpv6KeyAndValue()
		{
			Dictionary<string, string> map = new Dictionary<string, string>
			{
				["[2001:db8::10]:3000"] = "[2001:db8::1]:30001"
			};
			PortMapAddressTranslator translator = new PortMapAddressTranslator(map);

			Host mapped = translator.Translate(
				new Endpoint("BB1", "db1", "2001:db8::10", 3000, Endpoint.SourceList.ALTERNATE));

			Assert.AreEqual("2001:db8::1", mapped.name);
			Assert.AreEqual(30001, mapped.port);
			Assert.AreEqual("db1", mapped.tlsName);
		}

		[TestMethod]
		public void PortMapMissAndInvalidRejected()
		{
			PortMapAddressTranslator translator = new PortMapAddressTranslator(
				new Dictionary<string, string> { ["10.0.0.1:3000"] = "lb.example.com:30001" });

			Assert.IsNull(translator.Translate(new Endpoint("BB1", null, "10.0.0.1", 4333, Endpoint.SourceList.STANDARD)));
			Assert.Throws<AerospikeException>(() => new PortMapAddressTranslator(null));
			Assert.Throws<AerospikeException>(() => new PortMapAddressTranslator(
				new Dictionary<string, string> { ["10.0.0.1:3000"] = "lb.example.com" }));
		}

		[TestMethod]
		public void TranslatePassThroughOnMissUsesAdvertised()
		{
			StaticMapAddressTranslator translator = new StaticMapAddressTranslator(null);
			Endpoint advertised = new Endpoint("N1", "tls", "adv", 3000, Endpoint.SourceList.STANDARD);

			Host host = Cluster.Translate(translator, TranslationMissMode.PASS_THROUGH, advertised);

			Assert.AreEqual("adv", host.name);
			Assert.AreEqual("tls", host.tlsName);
			Assert.AreEqual(3000, host.port);
		}

		[TestMethod]
		public void TranslateStrictMissThrows()
		{
			StaticMapAddressTranslator translator = new StaticMapAddressTranslator(null);
			Endpoint advertised = new Endpoint("N1", "tls", "adv", 3000, Endpoint.SourceList.ALTERNATE);

			AerospikeException.TranslationMiss ex = Assert.Throws<AerospikeException.TranslationMiss>(
				() => Cluster.Translate(translator, TranslationMissMode.STRICT, advertised));

			Assert.AreEqual(ResultCode.TRANSLATION_MISS, ex.Result);
			StringAssert.Contains(ex.Message, "N1");
			StringAssert.Contains(ex.Message, "alternate");
			StringAssert.Contains(ex.Message, "static-map");
		}

		[TestMethod]
		public void TranslatePreservesAdvertisedTlsName()
		{
			Dictionary<string, string> map = new Dictionary<string, string> { ["adv"] = "reachable" };
			StaticMapAddressTranslator translator = new StaticMapAddressTranslator(map);
			Endpoint advertised = new Endpoint("N1", "peer-tls", "adv", 3000, Endpoint.SourceList.STANDARD);

			Host host = Cluster.Translate(translator, TranslationMissMode.PASS_THROUGH, advertised);

			Assert.AreEqual("reachable", host.name);
			Assert.AreEqual("peer-tls", host.tlsName);
		}

		[TestMethod]
		public void MergePolicyMergePutsStaticFirst()
		{
			Host[] staticSeeds = [new Host("static", 3000)];
			Host[] discovered = [new Host("discovered", 3000)];
			SeedCandidateManager manager = CreateManager(staticSeeds, SeedMergePolicy.MERGE, discovered);

			Host[] merged = manager.Merge(discovered, true);

			Assert.AreEqual(2, merged.Length);
			Assert.AreEqual("static", merged[0].name);
			Assert.AreEqual("discovered", merged[1].name);
		}

		[TestMethod]
		public void MergePolicyDiscoveryOnlyUsesStaticWhenEmpty()
		{
			Host[] staticSeeds = [new Host("static", 3000)];
			SeedCandidateManager manager = CreateManager(staticSeeds, SeedMergePolicy.DISCOVERY_ONLY, []);

			Host[] emptyDiscovery = manager.Merge([], true);
			Assert.AreEqual(1, emptyDiscovery.Length);
			Assert.AreEqual("static", emptyDiscovery[0].name);

			Host[] withDiscovery = manager.Merge([new Host("d", 3001)], true);
			Assert.AreEqual(1, withDiscovery.Length);
			Assert.AreEqual("d", withDiscovery[0].name);
		}

		[TestMethod]
		public void MergePolicyReplaceDropsStaticAfterInit()
		{
			Host[] staticSeeds = [new Host("static", 3000)];
			SeedCandidateManager manager = CreateManager(staticSeeds, SeedMergePolicy.REPLACE, []);

			Host[] init = manager.Merge([new Host("d", 3001)], true);
			Assert.AreEqual(2, init.Length);

			Host[] after = manager.Merge([new Host("d2", 3002)], false);
			Assert.AreEqual(1, after.Length);
			Assert.AreEqual("d2", after[0].name);
		}

		[TestMethod]
		public void StaticManagerCreatesNoRefreshThread()
		{
			Host[] seeds = [new Host("127.0.0.1", 3000)];
			Host[] published = null;
			ClientPolicy policy = new ClientPolicy();
			StaticSeedCandidateProvider provider = new StaticSeedCandidateProvider(seeds);
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, seeds, hosts => published = hosts, Log.Context.Empty);

			manager.Start();
			manager.AwaitInitialRefresh();

			Assert.IsFalse(manager.HasRefreshThread);
			Assert.IsNotNull(published);
			Assert.AreEqual(1, published.Length);
			Assert.AreEqual("127.0.0.1", published[0].name);

			manager.Close();
		}

		[TestMethod]
		public void TendModeRejectsNonDeadlineProvider()
		{
			Host[] seeds = [new Host("127.0.0.1", 3000)];
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 30000,
				discoveryTendRefreshDeadlineMs = 100
			};

			AerospikeException ex = Assert.Throws<AerospikeException>(() =>
				new SeedCandidateManager(new NonDeadlinePeriodicProvider(), policy, seeds, _ => { }, Log.Context.Empty));

			StringAssert.Contains(ex.Message, "deadline");
		}

		[TestMethod]
		public void TendModeRejectsRefreshIntervalBelowTendInterval()
		{
			Host[] seeds = [new Host("127.0.0.1", 3000)];
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 500,
				discoveryTendRefreshDeadlineMs = 100
			};

			AerospikeException ex = Assert.Throws<AerospikeException>(() =>
				new SeedCandidateManager(new DeadlinePeriodicProvider(), policy, seeds, _ => { }, Log.Context.Empty));

			StringAssert.Contains(ex.Message, "refresh interval");
		}

		[TestMethod]
		public void TendModeOverrunCircuitOpensAfterThreeOverruns()
		{
			Host[] seeds = [new Host("init", 3000)];
			Host[] published = null;
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 1000,
				discoveryTendRefreshDeadlineMs = 1
			};

			SlowDeadlineProvider provider = new SlowDeadlineProvider(20);
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, seeds, hosts => published = hosts, Log.Context.Empty);

			manager.Start();
			Assert.AreEqual("init", manager.GetCandidates()[0].name);

			// Cadence: refreshInterval/tendInterval == 1, so every tendCount refreshes.
			manager.Tend(1, 1000);
			manager.Tend(2, 1000);
			manager.Tend(3, 1000);

			Assert.IsTrue(manager.IsDeadlineCircuitOpen);
			Assert.AreEqual("init", published[0].name);

			int callsAfterTrip = provider.Calls;
			manager.Tend(4, 1000);
			Assert.AreEqual(callsAfterTrip, provider.Calls);

			manager.Close();
		}

		[TestMethod]
		public void TranslationCollisionExceptionMessage()
		{
			Host host = new Host("shared", 3000);
			AerospikeException.TranslationCollision ex = new AerospikeException.TranslationCollision(
				"A", "B", host, "pass-through");

			Assert.AreEqual(ResultCode.TRANSLATION_COLLISION, ex.Result);
			StringAssert.Contains(ex.Message, "A");
			StringAssert.Contains(ex.Message, "B");
			StringAssert.Contains(ex.Message, "shared:3000");
		}

		[TestMethod]
		public void AlternateAddressMissingExceptionMessage()
		{
			Host host = new Host("127.0.0.1", 3000);
			AerospikeException.AlternateAddressMissing ex = new AerospikeException.AlternateAddressMissing("BB1", host);

			Assert.AreEqual(ResultCode.CLIENT_ERROR, ex.Result);
			StringAssert.Contains(ex.Message, "BB1");
			StringAssert.Contains(ex.Message, "alternate-access-address");
			StringAssert.Contains(ex.Message, "useServicesAlternate");
		}

		private static SeedCandidateManager CreateManager(Host[] staticSeeds, SeedMergePolicy merge, Host[] discovered)
		{
			ClientPolicy policy = new ClientPolicy { seedMergePolicy = merge };
			return new SeedCandidateManager(
				new StaticSeedCandidateProvider(discovered),
				policy,
				staticSeeds,
				_ => { },
				Log.Context.Empty);
		}

		private sealed class NonDeadlinePeriodicProvider : ISeedCandidateProvider
		{
			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx) => [];
			public bool NeedsPeriodicRefresh() => true;
			public bool SupportsDeadline() => false;
		}

		private sealed class DeadlinePeriodicProvider : ISeedCandidateProvider
		{
			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx) => [];
			public bool NeedsPeriodicRefresh() => true;
			public bool SupportsDeadline() => true;
		}

		private sealed class SlowDeadlineProvider : ISeedCandidateProvider
		{
			private readonly int sleepMs;
			public int Calls;

			public SlowDeadlineProvider(int sleepMs)
			{
				this.sleepMs = sleepMs;
			}

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				Calls++;
				Thread.Sleep(sleepMs);
				return [new Host("fresh", 3000)];
			}

			public bool NeedsPeriodicRefresh() => true;
			public bool SupportsDeadline() => true;
		}
	}
}
