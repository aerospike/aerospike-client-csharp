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
using System.Threading;
using System.Threading.Tasks;

namespace Aerospike.Client
{
	public sealed class NodeValidator
	{
		/// <summary>
		/// Delay between starting connects to successive addresses of one host (FR-49 / RFC 8305).
		/// </summary>
		internal const int StaggerDelayMs = 250;

		internal Node fallback;
		internal string name;
		internal string hostname;
		internal Host primaryHost;
		internal IPEndPoint primaryAddress;
		internal Connection primaryConn;
		internal byte[] sessionToken;
		internal DateTime? sessionExpiration;
		internal uint features;
		internal Version serverVersion;

		/// <summary>
		/// Return first valid node referenced by seed host aliases. In most cases, aliases
		/// reference a single node.  If round robin DNS configuration is used, the seed host
		/// may have several addresses that reference different nodes in the cluster.
		/// </summary>
		public Node SeedNode(Cluster cluster, Host host, Peers peers)
		{
			name = null;
			hostname = null;
			primaryHost = null;
			primaryAddress = null;
			primaryConn = null;
			sessionToken = null;
			sessionExpiration = null;
			features = 0;

			IPAddress[] addresses = Connection.GetHostAddresses(host.name, cluster.connectionTimeout);
			Exception exception = null;

			// Try all addresses because they might point to different nodes.
			foreach (IPAddress address in addresses)
			{
				try
				{
					ValidateAddress(cluster, address, host.tlsName, host.port, true);
					SetHostname(host, address);

					Node node = cluster.CreateNode(this, false);

					if (ValidatePeers(peers, node))
					{
						return node;
					}
				}
				catch (Exception e)
				{
					// Log exception and continue to next alias.
					if (Log.DebugEnabled())
					{
						Log.Debug(cluster.context, "Address " + address + ' ' + host.port + " failed: " + Util.GetErrorMessage(e));
					}

					if (exception == null)
					{
						exception = e;
					}
				}
			}

			// Fallback signifies node exists, but is suspect.
			// Return null so other seeds can be tried.
			if (fallback != null)
			{
				return null;
			}

			// Exception can't be null here because Connection.GetHostAddresses()
			// will throw exception if aliases length is zero.
			throw exception;
		}

		private bool ValidatePeers(Peers peers, Node node)
		{
			if (peers == null)
			{
				return true;
			}

			try
			{
				peers.refreshCount = 0;
				node.RefreshPeers(peers);
			}
			catch (Exception)
			{
				node.Close();
				throw;
			}

			if (node.peersCount == 0)
			{
				// Node is suspect because multiple seeds are used and node does not have any peers.
				if (fallback == null)
				{
					fallback = node;
				}
				else
				{
					node.Close();
				}
				return false;
			}

			// Node is valid. Drop fallback if it exists.
			if (fallback != null)
			{
				if (Log.InfoEnabled())
				{
					Log.Info(node.cluster.context, "Skip orphan node: " + fallback);
				}
				fallback.Close();
				fallback = null;
			}
			return true;
		}

		/// <summary>
		/// Verify that a host alias references a valid node. When the host resolves to several
		/// addresses, connects are started one at a time, the last validated address first, then in
		/// DNS order, the next one when the stagger delay passes or an earlier connect fails
		/// (FR-49). All connects end by min(connectTimeout, tendInterval).
		/// </summary>
		public void ValidateNode(Cluster cluster, Host host)
		{
			IPAddress[] addresses = Connection.GetHostAddresses(host.name, cluster.connectionTimeout);

			if (addresses.Length == 1)
			{
				ValidateAddress(cluster, addresses[0], host.tlsName, host.port, false, 0);
				SetHostname(host, addresses[0]);
				cluster.validAddresses[host.name + ':' + host.port] = addresses[0];
				return;
			}

			cluster.validAddresses.TryGetValue(host.name + ':' + host.port, out IPAddress preferred);
			List<IPAddress> remaining = new List<IPAddress>(addresses.Length);

			if (preferred != null)
			{
				foreach (IPAddress address in addresses)
				{
					if (address.Equals(preferred))
					{
						remaining.Add(address);
						break;
					}
				}
			}

			foreach (IPAddress address in addresses)
			{
				if (!address.Equals(preferred))
				{
					remaining.Add(address);
				}
			}

			int bound = cluster.connectionTimeout > 0
				? Math.Min(cluster.connectionTimeout, cluster.tendInterval)
				: cluster.tendInterval;
			int delayMs = Math.Min(StaggerDelayMs, Math.Max(1, bound / addresses.Length));
			long deadline = Stopwatch.GetTimestamp() +
				(long)(bound * (Stopwatch.Frequency / 1000.0));
			Exception exception = null;

			while (true)
			{
				IPAddress address = ConnectFirst(host, remaining, delayMs, deadline);

				if (address == null)
				{
					break;
				}

				try
				{
					ValidateAddress(cluster, address, host.tlsName, host.port, false, 0);
					SetHostname(host, address);
					cluster.validAddresses[host.name + ':' + host.port] = address;
					return;
				}
				catch (Exception e)
				{
					if (Log.DebugEnabled())
					{
						Log.Debug(cluster.context, "Address " + address + ' ' + host.port + " failed: " +
							Util.GetErrorMessage(e));
					}

					if (exception == null)
					{
						exception = e;
					}
				}
			}

			if (exception != null)
			{
				throw exception;
			}

			throw new AerospikeException.Connection("No connection to " + host + " completed for " +
				addresses.Length + " addresses within " + bound + "ms");
		}

		/// <summary>
		/// Stagger TCP connects across <paramref name="addresses"/> (FR-49) and return those that
		/// completed within <paramref name="boundMs"/>, preferred first then DNS order. Used by
		/// unit tests; production multi-address validation uses
		/// <see cref="ValidateNode(Cluster, Host)"/>.
		/// </summary>
		internal static IPAddress[] ConnectAll(int boundMs, int port, IPAddress preferred, IPAddress[] addresses)
		{
			List<IPAddress> remaining = new List<IPAddress>(addresses.Length);

			if (preferred != null)
			{
				foreach (IPAddress address in addresses)
				{
					if (address.Equals(preferred))
					{
						remaining.Add(address);
						break;
					}
				}
			}

			foreach (IPAddress address in addresses)
			{
				if (!address.Equals(preferred))
				{
					remaining.Add(address);
				}
			}

			int delayMs = Math.Min(StaggerDelayMs, Math.Max(1, boundMs / Math.Max(1, addresses.Length)));
			long deadline = Stopwatch.GetTimestamp() +
				(long)(boundMs * (Stopwatch.Frequency / 1000.0));
			Host host = new Host("connect-all", port);

			// One staggered race: return the first address that connects. Remaining in-flight
			// connects are closed by ConnectFirst (same as Java validateAny's first win).
			IPAddress winner = ConnectFirst(host, remaining, delayMs, deadline);

			if (winner == null)
			{
				throw new AerospikeException.Connection("No connection to port " + port + " completed for " +
					addresses.Length + " addresses within " + boundMs + "ms");
			}

			return new[] { winner };
		}

		/// <summary>
		/// Return the first address whose connect completes, or null when none completes by the
		/// deadline. The returned address and addresses whose connect failed are removed from
		/// <paramref name="addresses"/>.
		/// </summary>
		internal static IPAddress ConnectFirst(Host host, List<IPAddress> addresses, int delayMs, long deadlineTicks)
		{
			IPAddress connected = null;
			int next = 0;
			List<(IPAddress Address, Socket Socket, Task Task)> pending = new List<(IPAddress, Socket, Task)>();
			long nextStart = Stopwatch.GetTimestamp();

			try
			{
				while (connected == null)
				{
					long now = Stopwatch.GetTimestamp();

					if (now - deadlineTicks >= 0)
					{
						break;
					}

					if (next < addresses.Count && now - nextStart >= 0)
					{
						IPAddress address = addresses[next];
						int state = StartConnect(pending, address, host.port);

						if (state == 0) // CONNECTED immediately
						{
							connected = address;
							break;
						}

						if (state == 1) // PENDING
						{
							next++;
							nextStart = now + (long)(delayMs * (Stopwatch.Frequency / 1000.0));
						}
						else // FAILED
						{
							addresses.RemoveAt(next);
							nextStart = now;
						}
						continue;
					}

					if (pending.Count == 0)
					{
						break;
					}

					int waitMs;

					if (next < addresses.Count && nextStart - deadlineTicks < 0)
					{
						waitMs = (int)Math.Max(1,
							(nextStart - now) / (Stopwatch.Frequency / 1000.0));
					}
					else
					{
						waitMs = (int)Math.Max(1,
							(deadlineTicks - now) / (Stopwatch.Frequency / 1000.0));
					}

					List<Task> running = new List<Task>(pending.Count);

					foreach ((IPAddress Address, Socket Socket, Task Task) item in pending)
					{
						if (!item.Task.IsCompleted)
						{
							running.Add(item.Task);
						}
					}

					if (running.Count > 0)
					{
						try
						{
							Task.WhenAny(running).Wait(waitMs);
						}
						catch
						{
						}
					}
					else
					{
						Thread.Sleep(Math.Min(waitMs, 5));
					}

					for (int i = pending.Count - 1; i >= 0; i--)
					{
						(IPAddress Address, Socket Socket, Task Task) item = pending[i];

						if (!item.Task.IsCompleted)
						{
							continue;
						}

						pending.RemoveAt(i);

						try
						{
							item.Socket.Dispose();
						}
						catch
						{
						}

						if (item.Task.Status == TaskStatus.RanToCompletion)
						{
							if (connected == null)
							{
								connected = item.Address;
							}
						}
						else
						{
							if (Log.DebugEnabled() && item.Task.IsFaulted)
							{
								Log.Debug("Address " + item.Address + ' ' + host.port + " failed: " +
									Util.GetErrorMessage(item.Task.Exception.GetBaseException()));
							}

							int index = addresses.IndexOf(item.Address);

							if (index >= 0)
							{
								addresses.RemoveAt(index);

								if (index < next)
								{
									next--;
								}
							}

							nextStart = Stopwatch.GetTimestamp();
						}
					}
				}
			}
			finally
			{
				foreach ((IPAddress Address, Socket Socket, Task Task) item in pending)
				{
					try
					{
						item.Socket.Dispose();
					}
					catch
					{
					}
				}
			}

			if (connected != null)
			{
				addresses.Remove(connected);
			}

			return connected;
		}

		/// <summary>
		/// 0 = connected immediately, 1 = pending, 2 = failed.
		/// </summary>
		private static int StartConnect(
			List<(IPAddress Address, Socket Socket, Task Task)> pending,
			IPAddress address,
			int port)
		{
			Socket socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

			try
			{
				socket.NoDelay = true;
				Task task = ConnectSocketAsync(socket, new IPEndPoint(address, port));

				if (task.IsCompleted)
				{
					try
					{
						socket.Dispose();
					}
					catch
					{
					}

					if (task.Status == TaskStatus.RanToCompletion)
					{
						return 0;
					}

					if (Log.DebugEnabled() && task.IsFaulted)
					{
						Log.Debug("Address " + address + ' ' + port + " failed: " +
							Util.GetErrorMessage(task.Exception.GetBaseException()));
					}

					return 2;
				}

				pending.Add((address, socket, task));
				return 1;
			}
			catch (Exception e)
			{
				try
				{
					socket.Dispose();
				}
				catch
				{
				}

				if (Log.DebugEnabled())
				{
					Log.Debug("Address " + address + ' ' + port + " failed: " + Util.GetErrorMessage(e));
				}

				return 2;
			}
		}

		private static async Task ConnectSocketAsync(Socket socket, IPEndPoint endpoint)
		{
			await socket.ConnectAsync(endpoint).ConfigureAwait(false);
		}

		/// <summary>
		/// Verify that one address of a host references a valid node.
		/// </summary>
		internal void ValidateAlias(Cluster cluster, Host host, IPAddress address)
		{
			ValidateAlias(cluster, host, address, 0);
		}

		/// <summary>
		/// Verify that one address of a host references a valid node. Connect, login and info
		/// end within <paramref name="timeoutMs"/> when greater than 0; otherwise the cluster
		/// connect timeout is used (FR-48).
		/// </summary>
		internal void ValidateAlias(Cluster cluster, Host host, IPAddress address, int timeoutMs)
		{
			ValidateAddress(cluster, address, host.tlsName, host.port, false, timeoutMs);
			SetHostname(host, address);
		}

		private void SetHostname(Host host, IPAddress address)
		{
			hostname = (IsHostname(host.name) && address.Equals(primaryAddress.Address)) ? host.name : null;
		}

		private static bool IsHostname(string name)
		{
			if (name.IndexOf(':') >= 0)
			{
				return false;
			}

			foreach (char c in name)
			{
				if (c != '.' && !char.IsDigit(c))
				{
					return true;
				}
			}

			return false;
		}

		private static string BuildUserAgentCommand(Cluster cluster)
		{
			string appId = cluster.appId;

			if (string.IsNullOrEmpty(appId))
			{
				if (cluster.user?.Length > 0)
				{
					appId = ByteUtil.Utf8ToString(cluster.user, 0, cluster.user.Length);
				}
				else
				{
					appId = "not-set";
				}
			}

			Version clientVersion = new(cluster.client.clientVersion);
			string agentValue = $"1,csharp-{clientVersion},{appId}";
			string b64 = Convert.ToBase64String(ByteUtil.StringToUtf8(agentValue));
			return "user-agent-set:value=" + b64;
		}

		private void ValidateAddress(Cluster cluster, IPAddress address, string tlsName, int port, bool detectLoadBalancer)
		{
			ValidateAddress(cluster, address, tlsName, port, detectLoadBalancer, 0);
		}

		private void ValidateAddress(
			Cluster cluster,
			IPAddress address,
			string tlsName,
			int port,
			bool detectLoadBalancer,
			int timeoutMs)
		{
			int timeout = timeoutMs > 0 ? timeoutMs : cluster.connectionTimeout;
			IPEndPoint socketAddress = new IPEndPoint(address, port);
			Connection conn = (cluster.tlsPolicy != null) ?
				new TlsConnection(cluster, tlsName, socketAddress, timeout, null) :
				new Connection(socketAddress, timeout, null);

			try
			{
				if (cluster.authEnabled)
				{
					// Login
					AdminCommand admin = new AdminCommand(ThreadLocalData.GetBuffer(), 0);
					admin.Login(cluster, conn, out sessionToken, out sessionExpiration);

					if (cluster.tlsPolicy != null && cluster.tlsPolicy.forLoginOnly)
					{
						// Switch to using non-TLS socket.
						SwitchClear sc = new SwitchClear(cluster, conn, tlsName, sessionToken);
						conn.Close();
						address = sc.clearAddress;
						socketAddress = sc.clearSocketAddress;
						conn = sc.clearConn;

						// Disable load balancer detection since non-TLS address has already
						// been retrieved via service info command.
						detectLoadBalancer = false;
					}
				}

				List<string> commands = new(5)
				{
					"node",
					"partition-generation",
					"build"
				};

				bool hasClusterName = cluster.HasClusterName;

				if (hasClusterName)
				{
					commands.Add("cluster-name");
				}

				string addressCommand = null;

				if (detectLoadBalancer)
				{
					if (IPAddress.IsLoopback(address))
					{
						// Disable load balancer detection for localhost.
						detectLoadBalancer = false;
					}
				}

				if (detectLoadBalancer || cluster.useServicesAlternate)
				{
					// Seed may be load balancer with changing address. Determine real address.
					// The alternate address list must also contain the node itself.
					addressCommand = (cluster.tlsPolicy != null) ?
						cluster.useServicesAlternate ? "service-tls-alt" : "service-tls-std" :
						cluster.useServicesAlternate ? "service-clear-alt" : "service-clear-std";

					commands.Add(addressCommand);
				}

				// Issue commands.
				Dictionary<string, string> map = Info.Request(conn, commands);

				// Node returned results.
				this.primaryHost = new Host(address.ToString(), tlsName, port);
				this.primaryAddress = socketAddress;
				this.primaryConn = conn;

				ValidateNode(map);
				ValidatePartitionGeneration(map);
				SetServerBuildVersion(map);

				bool sendUserAgent = serverVersion >= Node.SERVER_VERSION_8_1;
				if (sendUserAgent)
				{
					Info.Request(conn, BuildUserAgentCommand(cluster));
				}

				SetFeatures(serverVersion);

				if (hasClusterName)
				{
					ValidateClusterName(cluster, map);
				}

				if (cluster.useServicesAlternate)
				{
					ValidateAlternateAddress(map, addressCommand);
				}

				if (detectLoadBalancer)
				{
					SetAddress(cluster, map, addressCommand, tlsName);
				}
			}
			catch (Exception)
			{
				conn.Close();
				throw;
			}
		}

		private void ValidateNode(Dictionary<string, string> map)
		{
			if (!map.TryGetValue("node", out this.name))
			{
				throw new AerospikeException.InvalidNode("Node name is null");
			}
		}

		private void ValidatePartitionGeneration(Dictionary<string, string> map)
		{
			string genString;
			int gen;

			if (!map.TryGetValue("partition-generation", out genString))
			{
				throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost + " did not return partition-generation");
			}

			try
			{
				gen = Convert.ToInt32(genString);
			}
			catch (Exception)
			{
				throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost + " returned invalid partition-generation: " + genString);
			}

			if (gen == -1)
			{
				throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost + " is not yet fully initialized");
			}
		}

		private void SetServerBuildVersion(Dictionary<string, string> map)
		{
			try
			{
				string versionString = map["build"];
				versionString = versionString.Split("-")[0];
				Version parsed = new(versionString);
				// Normalize to 4 components to ensure consistent comparisons.
				// Use Math.Max(0, ...) to handle versions with fewer than 4 components.
				this.serverVersion = new Version(
					parsed.Major,
					parsed.Minor,
					Math.Max(0, parsed.Build),
					Math.Max(0, parsed.Revision));
			}
			catch (Exception)
			{
				// Unexpected exception. Use defaults.
			}
		}

		private void SetFeatures(Version serverVersion)
		{
			if (serverVersion >= Node.SERVER_VERSION_PSCAN)
			{
				features |= Node.HAS_PARTITION_SCAN;
			}
			else
			{
				// This client requires partition scan support. Partition scans were first
				// supported in server version 4.9. Do not allow any server node into the
				// cluster that is running server version < 4.9.
				if ((this.features & Node.HAS_PARTITION_SCAN) == 0)
				{
					throw new AerospikeException("Node " + this.name + ' ' + this.primaryHost +
						" version < 4.9. This client requires server version >= 4.9");
				}
			}

			if (serverVersion >= Node.SERVER_VERSION_QUERY_SHOW)
			{
				features |= Node.HAS_QUERY_SHOW;
			}

			if (serverVersion >= Node.SERVER_VERSION_PQUERY_BATCH_ANY)
			{
				features |= Node.HAS_BATCH_ANY;
				features |= Node.HAS_PARTITION_QUERY;
			}

			if (serverVersion >= Node.SERVER_VERSION_8_1_2)
			{
				features |= Node.HAS_QUERY_OPS_PROJECTION_EXT;
			}
		}

		private void ValidateClusterName(Cluster cluster, Dictionary<string, string> map)
		{
			string id;

			if (!map.TryGetValue("cluster-name", out id) || !cluster.clusterName.Equals(id))
			{
				throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost + ' ' +
						" expected cluster name '" + cluster.clusterName + "' received '" + id + "'");
			}
		}

		private void ValidateAlternateAddress(Dictionary<string, string> map, string addressCommand)
		{
			string result;

			if (!map.TryGetValue(addressCommand, out result) || result == null || result.Length == 0)
			{
				throw new AerospikeException.AlternateAddressMissing(this.name, this.primaryHost);
			}
		}

		private void SetAddress(Cluster cluster, Dictionary<string, string> map, string addressCommand, string tlsName)
		{
			string result;

			if (!map.TryGetValue(addressCommand, out result) || result == null || result.Length == 0)
			{
				// Server does not support service level call (service-clear-std, ...).
				// Load balancer detection is not possible.
				return;
			}

			List<Host> hosts = Host.ParseServiceHosts(result);
			Host h;

			// Search real hosts for seed.
			foreach (Host host in hosts)
			{
				try
				{
					h = TranslateServiceHost(cluster, host);
				}
				catch (AerospikeException.TranslationMiss tm)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn(cluster.context, tm.Message);
					}
					continue;
				}

				if (h.Equals(this.primaryHost))
				{
					// Found seed which is not a load balancer.
					return;
				}
			}

			// Seed not found, so seed is probably a load balancer.
			// Find first valid real host.
			foreach (Host host in hosts)
			{
				try
				{
					try
					{
						h = TranslateServiceHost(cluster, host);
					}
					catch (AerospikeException.TranslationMiss tm)
					{
						if (Log.WarnEnabled())
						{
							Log.Warn(cluster.context, tm.Message);
						}
						continue;
					}

					IPAddress[] addresses = Connection.GetHostAddresses(h.name, cluster.connectionTimeout);

					foreach (IPAddress address in addresses)
					{
						try
						{
							IPEndPoint socketAddress = new IPEndPoint(address, h.port);
							Connection conn = (cluster.tlsPolicy != null) ?
								new TlsConnection(cluster, tlsName, socketAddress, cluster.connectionTimeout, null) :
								new Connection(socketAddress, cluster.connectionTimeout, null);

							try
							{
								if (this.sessionToken != null)
								{
									if (!AdminCommand.Authenticate(cluster, conn, this.sessionToken))
									{
										throw new AerospikeException("Authentication failed");
									}
								}

								// Authenticated connection.  Set real host.
								this.primaryHost = new Host(address.ToString(), tlsName, h.port);
								this.primaryAddress = socketAddress;
								this.primaryConn.Close();
								this.primaryConn = conn;
								return;
							}
							catch (Exception)
							{
								conn.Close();
							}
						}
						catch (Exception)
						{
							// Try next address.
						}
					}
				}
				catch (Exception)
				{
					// Try next host.
				}
			}

			// Failed to find a valid address. IP Address is probably internal on the cloud
			// because the server access-address is not configured.  Log warning and continue
			// with original seed.
			if (Log.InfoEnabled())
			{
				Log.Info(cluster.context, "Invalid address " + result + ". access-address is probably not configured on server.");
			}
		}

		private Host TranslateServiceHost(Cluster cluster, Host host)
		{
			string tls = host.tlsName ?? primaryHost?.tlsName;
			return cluster.TranslateAddress(name, tls, host.name, host.port);
		}
	}

	sealed class SwitchClear
	{
		internal IPAddress clearAddress;
		internal IPEndPoint clearSocketAddress;
		internal Connection clearConn;

		// Switch from TLS connection to non-TLS connection.
		internal SwitchClear(Cluster cluster, Connection conn, string tlsName, byte[] sessionToken)
		{
			// Obtain non-TLS addresses.
			string command = cluster.useServicesAlternate ? "service-clear-alt" : "service-clear-std";
			Dictionary<string, string> map = Info.Request(conn, command, "node");

			if (!map.TryGetValue(command, out string result) || result == null)
			{
				throw new AerospikeException("Invalid non-TLS address response for " + command);
			}

			map.TryGetValue("node", out string nodeName);
			List<Host> hosts = Host.ParseServiceHosts(result);
			Host clearHost;

			// Find first valid non-TLS host.
			foreach (Host host in hosts)
			{
				try
				{
					try
					{
						clearHost = cluster.TranslateAddress(nodeName, tlsName, host.name, host.port);
					}
					catch (AerospikeException.TranslationMiss tm)
					{
						if (Log.WarnEnabled())
						{
							Log.Warn(cluster.context, tm.Message);
						}
						continue;
					}

					IPAddress[] addresses = Connection.GetHostAddresses(clearHost.name, cluster.connectionTimeout);

					foreach (IPAddress ia in addresses)
					{
						try
						{
							clearAddress = ia;
							clearSocketAddress = new IPEndPoint(ia, clearHost.port);
							clearConn = new Connection(clearSocketAddress, cluster.connectionTimeout, null);

							try
							{
								if (sessionToken != null)
								{
									if (!AdminCommand.Authenticate(cluster, clearConn, sessionToken))
									{
										throw new AerospikeException("Authentication failed");
									}
								}
								return; // Authenticated clear connection.
							}
							catch (Exception)
							{
								clearConn.Close();
							}
						}
						catch (Exception)
						{
							// Try next address.
						}
					}
				}
				catch (Exception)
				{
					// Try next host.
				}
			}
			throw new AerospikeException("Invalid non-TLS address: " + result);
		}
	}
}
