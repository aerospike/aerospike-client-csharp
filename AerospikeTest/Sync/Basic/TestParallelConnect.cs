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
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Multi-address connect races the TCP handshakes and returns completed addresses within
	/// min(connectTimeout, tendInterval), so one hung address does not cost a full connect timeout.
	/// </summary>
	[TestClass]
	public class TestParallelConnect
	{
		private const long MarginMs = 250;

		[TestMethod]
		public void HungAddressDoesNotDelayServingAddress()
		{
			IPAddress serving = IPAddress.Loopback;
			IPAddress hung = IPAddress.Parse("192.0.2.1");
			TcpListener listener = new TcpListener(serving, 0);
			listener.Start();
			int port = ((IPEndPoint)listener.LocalEndpoint).Port;

			try
			{
				Task accept = listener.AcceptTcpClientAsync().ContinueWith(t =>
				{
					try
					{
						t.Result.Close();
					}
					catch
					{
					}
				});

				Stopwatch sw = Stopwatch.StartNew();
				IPAddress[] connected = NodeValidator.ConnectAll(3000, port, null, new[] { hung, serving });
				long elapsed = sw.ElapsedMilliseconds;

				Assert.IsTrue(elapsed < 250 + MarginMs, "ConnectAll took " + elapsed + "ms");
				CollectionAssert.Contains(connected, serving);
				Assert.IsFalse(connected.Contains(hung));
				Assert.IsTrue(accept.Wait(2000));
			}
			finally
			{
				listener.Stop();
			}
		}

		[TestMethod]
		public void EveryAddressHungFailsWithinBound()
		{
			IPAddress first = IPAddress.Parse("192.0.2.1");
			IPAddress second = IPAddress.Parse("192.0.2.2");
			int port = FreePort();
			int boundMs = 250;

			Stopwatch sw = Stopwatch.StartNew();

			try
			{
				NodeValidator.ConnectAll(boundMs, port, null, new[] { first, second });
				Assert.Fail("ConnectAll succeeded");
			}
			catch (AerospikeException.Connection e)
			{
				long elapsed = sw.ElapsedMilliseconds;
				Assert.IsTrue(elapsed < boundMs + MarginMs, "ConnectAll took " + elapsed + "ms: " + e.Message);
				Assert.IsTrue(e.Message.Contains("within " + boundMs + "ms"), e.Message);
			}
		}

		[TestMethod]
		public void PreferredAddressIsTriedFirstInResultOrder()
		{
			IPAddress first = IPAddress.Loopback;
			IPAddress second = IPAddress.IPv6Loopback;
			TcpListener v4 = new TcpListener(first, 0);
			v4.Start();
			int port = ((IPEndPoint)v4.LocalEndpoint).Port;
			TcpListener v6 = null;

			try
			{
				try
				{
					v6 = new TcpListener(second, port);
					v6.Start();
				}
				catch
				{
					Assert.Inconclusive("IPv6 loopback not available");
					return;
				}

				Task accept4 = AcceptAndClose(v4);
				Task accept6 = AcceptAndClose(v6);

				IPAddress[] connected = NodeValidator.ConnectAll(1000, port, second, new[] { first, second });
				Assert.AreEqual(second, connected[0]);
				Assert.IsTrue(accept4.Wait(2000) || accept6.Wait(1));
			}
			finally
			{
				v4.Stop();
				v6?.Stop();
			}
		}

		private static Task AcceptAndClose(TcpListener listener)
		{
			return listener.AcceptTcpClientAsync().ContinueWith(t =>
			{
				try
				{
					if (t.Status == TaskStatus.RanToCompletion)
					{
						t.Result.Close();
					}
				}
				catch
				{
				}
			});
		}

		private static int FreePort()
		{
			TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			int port = ((IPEndPoint)listener.LocalEndpoint).Port;
			listener.Stop();
			return port;
		}
	}
}
