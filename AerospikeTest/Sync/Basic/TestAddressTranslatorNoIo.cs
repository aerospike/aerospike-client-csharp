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
using System.Net;
using System.Net.Sockets;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// U-2: built-in address translators perform translate-only work (no DNS / connect).
	/// JVM GuardedResolverProvider / GuardedProxySelector have no BCL equivalent; unresolvable
	/// host names and a small connect guard cover the same intent.
	/// </summary>
	[TestClass]
	public class TestAddressTranslatorNoIo
	{
		[TestMethod]
		public void PassThroughTranslateDoesNoNameLookupOrConnect()
		{
			PassThroughAddressTranslator translator = PassThroughAddressTranslator.Instance;

			Host name = translator.Translate(new Endpoint("BB9", "tls1", "node1.internal.invalid", 3000,
				Endpoint.SourceList.STANDARD));
			Host ipv6 = translator.Translate(new Endpoint("BB9", null, "2001:db8::1", 3001,
				Endpoint.SourceList.ALTERNATE));

			Assert.AreEqual("node1.internal.invalid", name.name);
			Assert.AreEqual("tls1", name.tlsName);
			Assert.AreEqual(3000, name.port);
			Assert.AreEqual("2001:db8::1", ipv6.name);
			Assert.AreEqual(3001, ipv6.port);
		}

		[TestMethod]
		public void StaticMapTranslateDoesNoNameLookup()
		{
			Dictionary<string, string> ipMap = new Dictionary<string, string>
			{
				["node1.internal"] = "node1.external.invalid"
			};
			StaticMapAddressTranslator translator = new StaticMapAddressTranslator(ipMap);

			Host hit = translator.Translate(new Endpoint("BB9", "tls1", "node1.internal", 3000,
				Endpoint.SourceList.STANDARD));
			Host miss = translator.Translate(new Endpoint("BB9", "tls1", "node2.internal", 3000,
				Endpoint.SourceList.ALTERNATE));

			Assert.IsNotNull(hit);
			Assert.AreEqual("node1.external.invalid", hit.name);
			Assert.IsNull(miss);
		}

		// Java StaticMapAddressTranslatorNoIoTest.guardDetectsLookup arms the JVM resolver.
		// There is no supported hook for Dns.GetHostAddresses in unit tests; translate-only checks above
		// use intentionally unresolvable names so a DNS lookup would throw or stall.

		[TestMethod]
		public void ConnectGuardDetectsEagerTranslator()
		{
			ConnectingTranslator connecting = new ConnectingTranslator();
			NoIoConnectGuard.Arm();

			try
			{
				Assert.Throws<AssertFailedException>(() =>
					connecting.Translate(new Endpoint("BB9", null, "127.0.0.1", 1, Endpoint.SourceList.STANDARD)));
			}
			finally
			{
				NoIoConnectGuard.Disarm();
			}

			Assert.AreEqual("Unexpected socket connect: 127.0.0.1:1", NoIoConnectGuard.LastMessage);
		}

		private sealed class ConnectingTranslator : IAddressTranslator
		{
			public Host Translate(Endpoint advertised)
			{
				using Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				NoIoConnectGuard.OnConnect(advertised.host, advertised.port);
				socket.Connect(new IPEndPoint(IPAddress.Loopback, advertised.port));
				return null;
			}
		}

		internal static class NoIoConnectGuard
		{
			internal static string LastMessage;
			private static bool armed;

			internal static void Arm() => armed = true;

			internal static void Disarm() => armed = false;

			internal static void OnConnect(string host, int port)
			{
				if (!armed)
				{
					return;
				}

				LastMessage = "Unexpected socket connect: " + host + ':' + port;
				throw new AssertFailedException(LastMessage);
			}
		}
	}
}
