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
using PodEndpoint = Aerospike.Test.L5Client.PodEndpoint;

namespace Aerospike.Test
{
	/// <summary>
	/// Both address shapes through the PrivateLink endpoint name, each with a fresh client seeded
	/// only at the endpoint name and the seed NodePort: pass-through of the alternate list every
	/// pod advertises, and a port-map from each node name to the endpoint name and that pod's
	/// NodePort. Row W-1 of the l5-aws harness.
	/// </summary>
	[TestClass]
	public class TestL5EndpointShapes
	{
		private const string Set = "l5-shapes";

		private L5Environment env;
		private K8sCluster cluster;
		private int foundSize;

		[TestInitialize]
		public void RequireEnvironment()
		{
			env = L5Environment.Require();
			env.WaitReady();
			env.WaitHealthy();
			cluster = K8sCluster.Find();
			foundSize = cluster.Size();
			Assert.AreEqual(L5Environment.ClusterSize, foundSize, "AerospikeCluster size");
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			if (cluster != null && env != null)
			{
				env.RestoreSize(cluster, foundSize);
			}
		}

		/// <summary>
		/// Pass-through of the alternate list: the client holds every pod at the endpoint name and
		/// the pod's own NodePort, and serves every partition.
		/// </summary>
		[TestMethod]
		public void PassThroughReachesEveryPodAtItsNodePort()
		{
			ClientPolicy policy = env.Policy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			AssertShape("pass-through", policy, L5Client.PodEndpoints(cluster));
		}

		/// <summary>
		/// Port-map from each node name to the endpoint name and that pod's NodePort, on the
		/// standard list and with a miss failing the peer.
		/// </summary>
		[TestMethod]
		public void PortMapByNodeNameReachesEveryPodAtItsNodePort()
		{
			SortedDictionary<string, PodEndpoint> endpoints = L5Client.PodEndpoints(cluster);
			Dictionary<string, string> portMap = new Dictionary<string, string>(StringComparer.Ordinal);

			foreach (PodEndpoint endpoint in endpoints.Values)
			{
				portMap[endpoint.NodeName] = env.Advertised(endpoint.NodePort);
			}

			Console.WriteLine("L5-PORT-MAP " + string.Join(", ",
				portMap.Select(e => e.Key + '=' + e.Value)));

			ClientPolicy policy = env.Policy();
			policy.useServicesAlternate = false;
			policy.addressTranslator = new PortMapAddressTranslator(portMap);
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertShape("port-map", policy, endpoints);
		}

		/// <summary>
		/// Every pod advertises the endpoint name verbatim with its own NodePort as its alternate
		/// address and in peers-clear-alt for every other pod.
		/// </summary>
		[TestMethod]
		public void EveryPodAdvertisesEndpointNameWithItsOwnNodePort()
		{
			SortedDictionary<string, PodEndpoint> endpoints = L5Client.PodEndpoints(cluster);
			ClientPolicy policy = env.Policy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			L5Client.CaptureLog();

			using (AerospikeClient client = new AerospikeClient(policy, env.Seed()))
			{
				L5Client.AwaitRouting("advertised", client, endpoints.Count, L5Client.RoutingWaitMs);
				L5Client.AssertAdvertisement("advertised", env, client, endpoints);
			}
		}

		/// <summary>
		/// The endpoint name resolves on the runner, and in the producer VPC only on the control
		/// run that makes it resolvable there.
		/// </summary>
		[TestMethod]
		public void EndpointNameResolvesOnlyWhereExpected()
		{
			string expect = "resolvable".Equals(env.Control) ? "yes" : "no";
			L5Environment.Result result = env.Ctl(L5Environment.CtlTimeoutMs, "resolve-check",
				"--expect-producer", expect);
			Console.WriteLine("L5-RESOLVE-CHECK control=" + env.Control + " expectProducer=" + expect +
				" exit=" + result.Exit + " lines=" + result.Tagged("L5-RESOLVE").Count);
			result.AssertSucceeded();
		}

		private void AssertShape(string shape, ClientPolicy policy,
			IReadOnlyDictionary<string, PodEndpoint> endpoints)
		{
			L5Client.CaptureLog();

			using (AerospikeClient client = new AerospikeClient(policy, env.Seed()))
			{
				Console.WriteLine("L5-SHAPE shape=" + shape + " seed=" + env.EndpointName + ':' +
					env.SeedPort + " useServicesAlternate=" + policy.useServicesAlternate +
					" translator=" + policy.addressTranslator.GetType().Name +
					" translationOnMiss=" + policy.translationOnMiss + " pods=" +
					string.Join(",", endpoints.Keys));
				L5Client.AwaitRouting(shape, client, L5Environment.ClusterSize, L5Client.RoutingWaitMs);
				L5Client.AssertNodesAtEndpoint(shape, env, client, endpoints);
				L5Client.AssertEveryPartitionServed(shape, client, Set);
			}
		}
	}
}
