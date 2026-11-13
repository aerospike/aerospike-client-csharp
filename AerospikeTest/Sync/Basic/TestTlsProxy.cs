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
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing-strategy N-6: TLS through a TCP pass-through listener. Started by
	/// AerospikeTest/tls/run_tls_matching.sh case n6_proxy.
	/// </summary>
	[TestClass]
	public class TestTlsProxy
	{
		private const int TimeoutMs = 5000;
		private const int KeyCount = 50;
		private static readonly ConcurrentQueue<string> Warnings = new ConcurrentQueue<string>();

		private static string tlsName;
		private static string hostName;
		private static string ns;
		private static string seedNode;
		private static readonly Dictionary<string, Forwarder> Forwarders = new Dictionary<string, Forwarder>();

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			tlsName = Required("TLS_NAME");
			hostName = Environment.GetEnvironmentVariable("TLS_HOST") ?? "127.0.0.1";
			ns = Environment.GetEnvironmentVariable("TLS_NAMESPACE") ?? "test";

			foreach (string entry in Required("TLS_NODES").Split(','))
			{
				string[] kv = entry.Split('=');
				if (seedNode == null)
				{
					seedNode = kv[0];
				}
				Forwarders[kv[0]] = new Forwarder(hostName, int.Parse(kv[1]));
			}

			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN)
				{
					Warnings.Enqueue(message);
				}
				Console.WriteLine("CLIENT-LOG " + level + ' ' + message);
			});
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
			foreach (Forwarder f in Forwarders.Values)
			{
				f.Dispose();
			}
			Log.SetCallback(null);
		}

		[TestMethod]
		public void ThroughPassThroughListener()
		{
			ClientPolicy cp = new ClientPolicy
			{
				tlsPolicy = new TlsPolicy(),
				timeout = TimeoutMs,
				failIfNotConnected = true,
				addressTranslator = new ForwarderTranslator(tlsName, Forwarders)
			};

			Host seed = new Host(Forwarder.Address, tlsName, Forwarders[seedNode].Port);

			using (AerospikeClient client = new AerospikeClient(cp, seed))
			{
				Assert.IsTrue(WaitForNodes(client, Forwarders.Count), "cluster did not discover both nodes");

				foreach (Node node in client.Nodes)
				{
					Host h = node.Host;
					Assert.IsTrue(Forwarders.TryGetValue(node.Name, out Forwarder f), "unexpected node " + node.Name);
					Console.WriteLine("TLS-PROXY node=" + node.Name + " host=" + h + " tlsName=" + h.tlsName +
						" forwarderConnections=" + f.Accepted);
					Assert.AreEqual(Forwarder.Address, h.name, "node " + node.Name + " endpoint");
					Assert.AreEqual(f.Port, h.port, "node " + node.Name + " port");
					Assert.AreEqual(tlsName, h.tlsName, "node " + node.Name + " tlsName");
					Assert.AreEqual(node.Name, Info.Request(node, "node"), "node " + node.Name + " info");
					Assert.IsTrue(f.Accepted > 0, "no connection through forwarder for " + node.Name);
				}

				for (int i = 0; i < KeyCount; i++)
				{
					Key key = new Key(ns, "tls", "proxy-" + i);
					client.Put(null, key, new Bin("v", i));
					Record rec = client.Get(null, key);
					Assert.IsNotNull(rec, "record " + i);
					Assert.AreEqual(i, rec.GetInt("v"), "record " + i);
				}
			}

			string quoted = "'" + tlsName + "'";
			long warnings = 0;

			foreach (string m in Warnings)
			{
				if (m.Contains(quoted))
				{
					warnings++;
				}
			}

			Assert.AreEqual(0L, warnings, "legacy match warnings");
		}

		private static bool WaitForNodes(AerospikeClient client, int count)
		{
			long deadline = Environment.TickCount64 + TimeoutMs;

			while (Environment.TickCount64 < deadline)
			{
				if (client.Nodes.Length == count)
				{
					return true;
				}
				Thread.Sleep(100);
			}

			return client.Nodes.Length == count;
		}

		private static string Required(string name)
		{
			string value = Environment.GetEnvironmentVariable(name);

			if (string.IsNullOrEmpty(value))
			{
				Assert.Inconclusive(name + " is not set. Run AerospikeTest/tls/run_tls_matching.sh n6_proxy");
			}

			return value;
		}

		private sealed class ForwarderTranslator : IAddressTranslator
		{
			private readonly string name;
			private readonly Dictionary<string, Forwarder> forwarders;

			public ForwarderTranslator(string tlsName, Dictionary<string, Forwarder> forwarders)
			{
				this.name = tlsName;
				this.forwarders = forwarders;
			}

			public Host Translate(Endpoint advertised)
			{
				if (forwarders.TryGetValue(advertised.nodeName, out Forwarder f))
				{
					return new Host(Forwarder.Address, name, f.Port);
				}

				return new Host(advertised.host, name, advertised.port);
			}
		}

		private sealed class Forwarder : IDisposable
		{
			public const string Address = "127.0.0.1";

			private readonly TcpListener server;
			private readonly string targetHost;
			private readonly int targetPort;
			private int accepted;
			private readonly List<Socket> sockets = new List<Socket>();
			private volatile bool closed;

			public Forwarder(string targetHost, int targetPort)
			{
				this.targetHost = targetHost;
				this.targetPort = targetPort;
				server = new TcpListener(IPAddress.Parse(Address), 0);
				server.Start();
				Thread t = new Thread(AcceptLoop) { IsBackground = true, Name = "forwarder-" + targetPort };
				t.Start();
			}

			public int Port => ((IPEndPoint)server.LocalEndpoint).Port;

			public int Accepted => Volatile.Read(ref accepted);

			private void AcceptLoop()
			{
				while (!closed)
				{
					try
					{
						Socket client = server.AcceptSocket();
						Socket target = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
						target.Connect(targetHost, targetPort);
						lock (sockets)
						{
							sockets.Add(client);
							sockets.Add(target);
						}
						Interlocked.Increment(ref accepted);
						Pump(client, target);
						Pump(target, client);
					}
					catch (Exception)
					{
						// Closed or target unreachable.
					}
				}
			}

			private static void Pump(Socket from, Socket to)
			{
				Thread t = new Thread(() =>
				{
					byte[] buf = new byte[8192];

					try
					{
						while (true)
						{
							int n = from.Receive(buf);

							if (n <= 0)
							{
								break;
							}

							to.Send(buf, n, SocketFlags.None);
						}
					}
					catch (Exception)
					{
						// Peer closed.
					}
					finally
					{
						try { from.Dispose(); } catch { }
						try { to.Dispose(); } catch { }
					}
				})
				{ IsBackground = true, Name = "forwarder-pump" };
				t.Start();
			}

			public void Dispose()
			{
				closed = true;

				try
				{
					server.Stop();
				}
				catch
				{
					// Ignore.
				}

				lock (sockets)
				{
					foreach (Socket s in sockets)
					{
						try { s.Dispose(); } catch { }
					}
				}
			}
		}
	}
}
