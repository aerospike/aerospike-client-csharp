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
using Driver = Aerospike.L5Driver.L5Driver;

namespace Aerospike.Test
{
	/// <summary>
	/// The L5 driver's watch probe against two loopback listeners under one test name. Needs no server.
	/// </summary>
	[TestClass]
	public class TestL5DriverProbe
	{
		private const string HostName = "probe.l5.test";

		[TestMethod]
		public void ServingAfterThreeRoundsWithEveryAddress()
		{
			IPAddress first = IPAddress.Loopback;
			IPAddress second = IPAddress.Parse("127.0.0.2");

			if (!CanBind(second))
			{
				second = IPAddress.IPv6Loopback;
			}

			if (!CanBind(second))
			{
				Assert.Inconclusive("Neither 127.0.0.2 nor ::1 can be bound locally");
				return;
			}

			IPAddress[] addresses = { first, second };
			int port = FreePort(first, second);
			Driver.Probe probe = new Driver.Probe(HostName, port, host =>
			{
				Assert.AreEqual(HostName, host);
				return addresses;
			});

			TcpListener a = new TcpListener(first, port);
			a.Start();

			try
			{
				probe.Round(0);
				probe.Round(10);
				Assert.IsNull(probe.ServingAt);
				Assert.AreEqual(10L, probe.LastClosedAt);

				TcpListener b = new TcpListener(second, port);
				b.Start();

				try
				{
					probe.Round(20);
					probe.Round(30);
					Assert.IsNull(probe.ServingAt);
					probe.Round(40);
					Assert.AreEqual(20L, probe.ServingAt);
					Assert.AreEqual(10L, probe.LastClosedAt);
				}
				finally
				{
					b.Stop();
				}
			}
			finally
			{
				a.Stop();
			}

			Assert.AreEqual(
				"[{\"address\":\"" + first + "\",\"firstConnectedAt\":0}," +
				"{\"address\":\"" + second + "\",\"firstConnectedAt\":20}]",
				probe.AddressesJson());
		}

		[TestMethod]
		public void AddressThatNeverConnectsIsNull()
		{
			IPAddress first = IPAddress.Loopback;
			int port;
			TcpListener listener = new TcpListener(first, 0);
			listener.Start();
			port = ((IPEndPoint)listener.LocalEndpoint).Port;
			listener.Stop();

			Driver.Probe probe = new Driver.Probe(HostName, port, _ => new[] { first });
			probe.Round(0);
			probe.Round(10);
			Assert.IsNull(probe.ServingAt);
			Assert.AreEqual(10L, probe.LastClosedAt);
			Assert.AreEqual(
				"[{\"address\":\"" + first + "\",\"firstConnectedAt\":null}]",
				probe.AddressesJson());
		}

		private static int FreePort(IPAddress first, IPAddress second)
		{
			for (int i = 0; i < 100; i++)
			{
				int port;
				TcpListener onSecond = new TcpListener(second, 0);
				onSecond.Start();
				port = ((IPEndPoint)onSecond.LocalEndpoint).Port;
				onSecond.Stop();

				try
				{
					TcpListener onFirst = new TcpListener(first, port);
					onFirst.Start();
					onFirst.Stop();
					return port;
				}
				catch (SocketException)
				{
				}
			}

			throw new IOException("No port free on both " + first + " and " + second);
		}

		private static bool CanBind(IPAddress address)
		{
			try
			{
				TcpListener listener = new TcpListener(address, 0);
				listener.Start();
				listener.Stop();
				return true;
			}
			catch (SocketException)
			{
				return false;
			}
		}
	}
}
