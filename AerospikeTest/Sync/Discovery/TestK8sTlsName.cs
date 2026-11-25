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
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using Aerospike.Client;
using Pod = Aerospike.Test.K8sCluster.Pod;

namespace Aerospike.Test
{
	/// <summary>
	/// TLS against an Aerospike cluster on the Aerospike Kubernetes Operator, where every pod mounts
	/// one shared certificate secret. The certificate names the cluster tls-name, not the endpoint
	/// hostname the client dials, so the client must validate each node against the tls-name the
	/// cluster advertises.
	/// </summary>
	[TestClass]
	public class TestK8sTlsName
	{
		private const string Set = "l3-tls-name";
		private const int MinPods = 2;
		private const long RoutingWaitMs = 120000;
		private const int KeySearchLimit = 100000;
		private static readonly Regex Peer = new Regex("\\[([0-9A-Fa-f]+),([^,\\[\\]]*),\\[((?:[^\\[\\]]|\\[[^\\]]*\\])*)\\]\\]",
			RegexOptions.Compiled);
		private const string TlsProfile = " needs DISCOVERY_TLS_CA_FILE and DISCOVERY_TLS_NAME, set once " +
			".github/discovery/enable-l3-tls.sh serves TLS on the AerospikeCluster (run_l3.sh L3_PROFILE=tls, or the " +
			"discovery-k8s workflow)";

		private static string ns;

		private K8sCluster scaled;
		private int scaledFrom;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			ns = !string.IsNullOrEmpty(SuiteHelpers.ns) ? SuiteHelpers.ns : "test";
		}

		[TestInitialize]
		public void RequireCluster()
		{
			if (string.IsNullOrEmpty(K8sCluster.Kubeconfig))
			{
				Assert.Inconclusive("Kubernetes TLS name " + K8sCluster.Skip);
			}

			if (string.IsNullOrEmpty(K8sCluster.TlsCaFile) || string.IsNullOrEmpty(K8sCluster.TlsName))
			{
				Assert.Inconclusive("Kubernetes TLS name" + TlsProfile);
			}
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			if (scaled != null)
			{
				K8sCluster cluster = scaled;
				scaled = null;
				cluster.Scale(scaledFrom);
			}
		}

		[TestMethod]
		public void SharedCertificateCarriesTlsNameAndClientValidatesAgainstIt()
		{
			K8sCluster cluster = K8sCluster.Find();
			string endpoint = K8sCluster.EndpointHostname;
			string tlsName = K8sCluster.TlsName;
			Assert.IsFalse(string.IsNullOrEmpty(endpoint), "DISCOVERY_ENDPOINT_HOSTNAME is not set");
			Assert.AreEqual(tlsName, cluster.Get(".spec.aerospikeConfig.network.service['tls-name']"), "AerospikeCluster tls-name");
			Assert.AreEqual(cluster.Get(".spec.aerospikeNetworkPolicy.alternateAccess"),
				cluster.Get(".spec.aerospikeNetworkPolicy.tlsAlternateAccess"));

			int size = cluster.Size();

			if (size < MinPods)
			{
				scaled = cluster;
				scaledFrom = size;
				cluster.Scale(MinPods);
			}

			AssertCertificate(cluster, tlsName, endpoint);

			int tlsPort = int.Parse(cluster.Get(".spec.aerospikeConfig.network.service['tls-port']"));
			List<Pod> pods = cluster.Pods();
			Dictionary<string, Pod> podsByIp = new Dictionary<string, Pod>();
			SortedDictionary<string, string> expected = new SortedDictionary<string, string>(StringComparer.Ordinal);

			foreach (Pod pod in pods)
			{
				int nodePort = cluster.NodePort(pod.Name, tlsPort);
				Console.WriteLine("L3-TLS-POD " + pod + " tlsServiceNodePort=" + nodePort);
				Assert.IsTrue(nodePort > 0, pod.Name + " has no Service NodePort for " + tlsPort);
				podsByIp[pod.Ip] = pod;
				expected[pod.Name] = endpoint + ':' + nodePort;
			}

			ClientPolicy policy = Policy();
			int seedPort = cluster.NodePort(pods[0].Name, tlsPort);
			Host seed = new Host(endpoint, tlsName, seedPort);
			CaptureLog();

			using (AerospikeClient client = new AerospikeClient(policy, seed))
			{
				Node[] masters = new Node[Node.PARTITIONS];
				bool routed = K8sCluster.WaitFor(() => client.Nodes.Length == pods.Count &&
					Unowned(client, masters) == 0 && Owners(masters).Count == pods.Count, RoutingWaitMs, 500);
				Console.WriteLine("L3-TLS-ROUTING seed=" + seed + " pods=" + pods.Count + " nodes=" +
					client.Nodes.Length + " partitions=" + Node.PARTITIONS + " unowned=" + Unowned(client, masters) +
					" owners=" + TestL2Topology.FmtMap(Owners(masters)));
				Assert.IsTrue(routed, "Client seeded at " + seed + " did not hold " + pods.Count +
					" nodes owning every partition within " + RoutingWaitMs + "ms");

				SortedSet<string> nodeNames = new SortedSet<string>(StringComparer.Ordinal);
				SortedSet<string> advertisedPeers = new SortedSet<string>(StringComparer.Ordinal);
				SortedSet<string> matched = new SortedSet<string>(StringComparer.Ordinal);

				foreach (Node node in client.Nodes)
				{
					nodeNames.Add(node.Name);
				}

				foreach (Node node in client.Nodes)
				{
					Dictionary<string, string> info = Info.Request(null, node, "service-tls-std", "service-tls-alt", "peers-tls-alt");
					Pod pod = PodOf(podsByIp, Get(info, "service-tls-std"));
					Assert.IsNotNull(pod, node.Name + " TLS access " + Get(info, "service-tls-std") + " is no pod IP in " +
						TestL2Topology.Fmt(podsByIp.Keys));
					Assert.IsTrue(matched.Add(pod.Name), pod.Name + " matched twice");
					List<string[]> peers = ParsePeers(Get(info, "peers-tls-alt"));
					Console.WriteLine("L3-TLS-NAME pod=" + pod.Name + " node=" + node.Name + " tlsAlternate=" +
						Get(info, "service-tls-alt") + " expected=" + expected[pod.Name] + " clientTlsName=" +
						node.Host.tlsName + " peersTlsAlt=" + Get(info, "peers-tls-alt"));
					Assert.AreEqual(expected[pod.Name], Get(info, "service-tls-alt"), pod.Name + " TLS alternate address");
					Assert.AreEqual(tlsName, node.Host.tlsName, node.Name + " client tls-name");
					Assert.AreEqual(pods.Count - 1, peers.Count, pod.Name + " peers " + Get(info, "peers-tls-alt"));

					foreach (string[] peer in peers)
					{
						Assert.AreEqual(tlsName, peer[1], pod.Name + " tls-name for peer " + peer[0]);
						Assert.IsTrue(nodeNames.Contains(peer[0]),
							pod.Name + " advertises unknown peer " + peer[0] + ", nodes " + TestL2Topology.Fmt(nodeNames));
						advertisedPeers.Add(peer[0]);
					}
				}
				CollectionAssert.AreEquivalent(expected.Keys.ToList(), matched.ToList());
				CollectionAssert.AreEquivalent(nodeNames.ToList(), advertisedPeers.ToList(), "Nodes advertised as peers");
				AssertServedByEveryNode(client, masters);
			}

			Host fqdn = new Host(endpoint, endpoint, seedPort);

			try
			{
				using (AerospikeClient client = new AerospikeClient(policy, fqdn))
				{
					List<string> nodeList = new List<string>();

					foreach (Node node in client.Nodes)
					{
						nodeList.Add(node.Name);
					}
					Assert.Fail("TLS handshake with " + fqdn + " accepted a certificate that names only " + tlsName +
						"; nodes=" + TestL2Topology.Fmt(nodeList));
				}
			}
			catch (AerospikeException ae)
			{
				string message = Messages(ae);
				Console.WriteLine("L3-TLS-FQDN-REJECTED seed=" + fqdn + " error=" +
					Regex.Replace(message, "\\s*\\n\\s*", " "));
				Assert.IsTrue(message.Contains("Invalid TLS name: " + endpoint), message);
			}
		}

		private static void AssertCertificate(K8sCluster cluster, string tlsName, string endpoint)
		{
			string certFile = cluster.Get(".spec.aerospikeConfig.network.tls[?(@.name==\"" + tlsName + "\")]['cert-file']");
			int index = certFile.LastIndexOf('/');
			Assert.IsTrue(index > 0, "cert-file " + certFile);
			string dir = certFile.Substring(0, index);
			string secret = cluster.Get(".spec.storage.volumes[?(@.aerospike.path==\"" + dir + "\")].source.secret.secretName");
			Assert.IsFalse(string.IsNullOrEmpty(secret), "No secret volume at " + dir);

			X509Certificate2 cert = new X509Certificate2(cluster.SecretData(secret, certFile.Substring(index + 1)));
			List<string> dnsNames = new List<string>();
			List<string> sans = new List<string>();
			X509SubjectAlternativeNameExtension sanExtension = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();

			if (sanExtension != null)
			{
				foreach (string dns in sanExtension.EnumerateDnsNames())
				{
					sans.Add("2:" + dns);
					dnsNames.Add(dns);
				}

				foreach (System.Net.IPAddress ip in sanExtension.EnumerateIPAddresses())
				{
					sans.Add("7:" + ip);
				}
			}

			Console.WriteLine("L3-TLS-CERT secret=" + cluster.Namespace + '/' + secret + " key=" +
				certFile.Substring(index + 1) + " subject=" + cert.Subject + " sans=" + TestL2Topology.Fmt(sans) +
				" dnsNames=" + TestL2Topology.Fmt(dnsNames) + " endpoint=" + endpoint);
			CollectionAssert.AreEqual(new List<string> { tlsName }, dnsNames, "dNSName SANs");

			if (sanExtension != null)
			{
				foreach (string dns in sanExtension.EnumerateDnsNames())
				{
					Assert.IsFalse(endpoint.Equals(dns, StringComparison.OrdinalIgnoreCase),
						"SAN 2:" + dns + " names the endpoint " + endpoint);
				}

				foreach (System.Net.IPAddress ip in sanExtension.EnumerateIPAddresses())
				{
					string ipText = ip.ToString();
					Assert.IsFalse(endpoint.Equals(ipText, StringComparison.OrdinalIgnoreCase),
						"SAN 7:" + ipText + " names the endpoint " + endpoint);
				}
			}
		}

		private static void AssertServedByEveryNode(AerospikeClient client, Node[] masters)
		{
			WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
			wp.replica = Replica.MASTER;
			Policy rp = new Policy(client.ReadPolicyDefault);
			rp.replica = Replica.MASTER;

			foreach (Node node in client.Nodes)
			{
				Key key = null;

				for (int i = 0; i < KeySearchLimit && key == null; i++)
				{
					Key candidate = new Key(ns, Set, "tls-" + i);

					if (ReferenceEquals(masters[(int)Partition.GetPartitionId(candidate.digest)], node))
					{
						key = candidate;
					}
				}
				Assert.IsNotNull(key, node.Name + " is master of no partition");
				Assert.AreSame(node, Partition.Write(client.Cluster, wp, key).GetNodeWrite(client.Cluster));
				client.Put(wp, key, new Bin("v", node.Name));
				Assert.AreEqual(node.Name, client.Get(rp, key).GetString("v"));
				Console.WriteLine("L3-TLS-SERVED node=" + node.Name + " key=" + key.userKey + " partition=" +
					Partition.GetPartitionId(key.digest) + " write=ok read=ok");
			}
		}

		private static ClientPolicy Policy()
		{
			string password = Environment.GetEnvironmentVariable("AEROSPIKE_ADMIN_PASSWORD");
			Assert.IsFalse(string.IsNullOrEmpty(password), "AEROSPIKE_ADMIN_PASSWORD is required");
			ClientPolicy policy = new ClientPolicy();
			policy.user = "admin";
			policy.password = password;
			policy.authMode = AuthMode.INTERNAL;
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			policy.tlsPolicy = new TlsPolicy();
			return policy;
		}

		private static List<string[]> ParsePeers(string reply)
		{
			List<string[]> peers = new List<string[]>();

			if (reply == null)
			{
				return peers;
			}

			Match match = Peer.Match(reply);

			while (match.Success)
			{
				peers.Add(new string[] { match.Groups[1].Value, match.Groups[2].Value });
				match = match.NextMatch();
			}
			return peers;
		}

		private static int Unowned(AerospikeClient client, Node[] masters)
		{
			Partitions partitions = client.Cluster.partitionMap[ns];
			int unowned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = partitions == null ? null : partitions.replicas[0][i];

				if (node == null || !node.Active || !ReferenceEquals(client.Cluster.GetNode(node.Name), node))
				{
					masters[i] = null;
					unowned++;
				}
				else
				{
					masters[i] = node;
				}
			}
			return unowned;
		}

		private static Dictionary<string, int> Owners(Node[] masters)
		{
			SortedDictionary<string, int> owners = new SortedDictionary<string, int>(StringComparer.Ordinal);

			foreach (Node node in masters)
			{
				if (node != null)
				{
					owners[node.Name] = owners.GetValueOrDefault(node.Name, 0) + 1;
				}
			}
			return new Dictionary<string, int>(owners);
		}

		private static Pod PodOf(Dictionary<string, Pod> podsByIp, string access)
		{
			if (access == null)
			{
				return null;
			}

			foreach (Host h in Host.ParseServiceHosts(access))
			{
				if (podsByIp.TryGetValue(h.name, out Pod pod))
				{
					return pod;
				}
			}
			return null;
		}

		private static string Messages(Exception t)
		{
			StringBuilder sb = new StringBuilder();

			for (Exception e = t; e != null; e = e.InnerException)
			{
				if (sb.Length > 0)
				{
					sb.Append(" <- ");
				}
				sb.Append(e.Message);
			}
			return sb.ToString();
		}

		private static void CaptureLog()
		{
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) => Console.WriteLine("CLIENT-LOG " + level + ' ' + message));
		}

		private static string Get(Dictionary<string, string> info, string key)
		{
			return info.TryGetValue(key, out string value) ? value : null;
		}
	}
}
