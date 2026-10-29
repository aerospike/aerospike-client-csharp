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
using L2Node = Aerospike.Test.L2Topology.L2Node;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3 row N-17 against the cn-only profile of test/discovery/run_l2.sh,
	/// ee only. The topology is N-6's, but the server certificate carries the tls-name only as its
	/// subject CN, with no matching dNSName SAN. In its own class and profile so that a failing N-17
	/// does not fail the other rows.
	/// </summary>
	[TestClass]
	public class TestL2CnOnlyCertificate
	{
		private const string PROFILE = " (run_l2.sh DISCOVERY_SUBSET=ee L2_PROFILE=cn-only)";

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);
		}

		/// <summary>
		/// N-17: a certificate that carries the tls-name only as its subject CN is rejected. The TLS
		/// handshake with the seed fails on the tls-name and no <see cref="Node"/>
		/// is created.
		/// </summary>
		[TestMethod]
		public void N17CnOnlyCertificateIsRejected()
		{
			TestL2Topology.SkipUnless("ee".Equals(L2Topology.Subset), "N-17 is ee only: it needs server TLS and TLS listeners" +
				PROFILE + "; subset is " + L2Topology.Subset);
			TestL2Topology.SkipUnless("cn-only".Equals(L2Topology.TlsCertShape), "N-17 needs DISCOVERY_TLS_CERT_SHAPE=cn-only, " +
				"a server certificate with the tls-name only as its CN" + PROFILE + "; shape is " + L2Topology.TlsCertShape);

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			Assert.IsTrue(first.TlsPort > 0, "No TLS listener for " + first);
			Assert.IsNotNull(first.TlsName, "No tls-name for " + first);

			ClientPolicy policy = L2Topology.Policy();
			policy.tlsPolicy = new TlsPolicy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.TlsPortMap(nodes));
			TestL2Topology.CaptureWarnings();

			try
			{
				using (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.Host, first.TlsName, first.TlsPort)))
				{
					Console.WriteLine("L2-N17-ACCEPTED nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) + " cnFallbackWarnings=" +
						TestL2Topology.Fmt(TestL2Topology.CnFallbackWarnings()));
					Assert.Fail("N-17: TLS handshake accepted a certificate that carries " + first.TlsName + " only as its CN; nodes=" +
						TestL2Topology.Fmt(L2Topology.NodeNames(client)) + " cnFallbackWarnings=" +
						TestL2Topology.Fmt(TestL2Topology.CnFallbackWarnings()));
				}
			}
			catch (AerospikeException ae)
			{
				// The Java client reports "Invalid TLS name: <name>". The .NET client surfaces the SslStream
				// certificate name mismatch, so the message is recorded but not matched against Java's text.
				string message = TestL2Topology.Messages(ae);
				Console.WriteLine("L2-N17-ERROR " + message);
			}
		}
	}
}
