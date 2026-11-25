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
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Aerospike.Client
{
	/// <summary>
	/// Seed candidate provider that resolves one or more DNS names to seed hosts on every refresh.
	/// Use it when the seed hosts are published in DNS, including the split-network case where a seed
	/// name resolves only on the client's network to addresses the client can reach.
	/// <para>
	/// Record types:
	/// <list type="bullet">
	/// <item><see cref="RecordType.A"/>: A and AAAA records. One host per address, with
	/// <see cref="Builder.Port"/>.</item>
	/// <item><see cref="RecordType.SRV"/>: SRV records. One host per record with the record's port,
	/// ordered by priority then weight. Each target is resolved with A, then AAAA.</item>
	/// </list>
	/// </para>
	/// <para>
	/// Every returned host is an IP address literal. All I/O of one refresh is bounded by
	/// <see cref="Builder.Timeout"/>. The provider starts no thread. Eligible for
	/// <see cref="DiscoveryExecutionMode.TEND"/> when timeout does not exceed
	/// <see cref="ClientPolicy.discoveryTendRefreshDeadlineMs"/>.
	/// </para>
	/// </summary>
	public sealed class DnsSeedCandidateProvider : ISeedCandidateProvider
	{
		/// <summary>
		/// DNS record type queried for each name.
		/// </summary>
		public enum RecordType
		{
			/// <summary>A and AAAA records.</summary>
			A,

			/// <summary>SRV records. Targets are resolved with A, then AAAA, queries.</summary>
			SRV
		}

		/// <summary>Default port for A and AAAA results.</summary>
		public const int DefaultPort = 3000;

		/// <summary>Default refresh timeout in milliseconds.</summary>
		public const int DefaultTimeout = 80;

		private const int DnsPort = 53;

		private readonly List<string> names;
		private readonly RecordType recordType;
		private readonly int port;
		private readonly string tlsName;
		private readonly int timeout;
		private readonly List<string> servers;
		private readonly List<IPEndPoint> resolvers;

		/// <summary>
		/// Return a builder.
		/// </summary>
		public static Builder CreateBuilder()
		{
			return new Builder();
		}

		private DnsSeedCandidateProvider(Builder builder)
		{
			if (builder.names == null || builder.names.Count == 0)
			{
				throw new AerospikeException("Invalid DNS seed provider names: at least one name is required");
			}

			HashSet<string> set = new HashSet<string>();
			List<string> ordered = new List<string>();

			foreach (string name in builder.names)
			{
				if (name == null || name.Length == 0 || name.Equals(".") || name.Contains(" "))
				{
					throw new AerospikeException("Invalid DNS seed provider name: '" + name + "'");
				}

				string absolute = Absolute(name).ToLowerInvariant();

				if (!set.Add(absolute))
				{
					throw new AerospikeException("Invalid DNS seed provider names: duplicate name '" + name + "'");
				}

				ordered.Add(name);
			}

			if (builder.port <= 0 || builder.port > 65535)
			{
				throw new AerospikeException("Invalid DNS seed provider port: " + builder.port);
			}

			if (builder.tlsName != null && builder.tlsName.Length == 0)
			{
				throw new AerospikeException("Invalid DNS seed provider tlsName: empty");
			}

			if (builder.timeout <= 0)
			{
				throw new AerospikeException("Invalid DNS seed provider timeout: " + builder.timeout);
			}

			this.names = ordered;
			this.recordType = builder.recordType;
			this.port = builder.port;
			this.tlsName = builder.tlsName;
			this.timeout = builder.timeout;

			if (builder.servers != null && builder.servers.Count > 0)
			{
				List<string> serverList = new List<string>();
				List<IPEndPoint> ends = new List<IPEndPoint>();

				foreach (string server in builder.servers)
				{
					ends.Add(ParseServer(server));
					serverList.Add(server);
				}

				this.servers = serverList;
				this.resolvers = ends;
			}
			else
			{
				this.servers = new List<string>();
				this.resolvers = SystemResolvers();
			}
		}

		/// <inheritdoc/>
		public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
		{
			long deadline = Stopwatch.GetTimestamp() + timeout * Stopwatch.Frequency / 1000;
			HostSet hosts = new HostSet();
			Dictionary<string, string> failures = new Dictionary<string, string>();

			foreach (string name in names)
			{
				try
				{
					List<Host> list = (recordType == RecordType.SRV) ?
						ResolveSrv(name, deadline) : ResolveA(name, deadline);

					if (list.Count == 0)
					{
						failures[name] = "empty answer";
					}

					foreach (Host host in list)
					{
						hosts.Add(host);
					}
				}
				catch (Exception e)
				{
					failures[name] = Reason(e);
				}
			}

			if (hosts.Count == 0)
			{
				System.Text.StringBuilder sb = new System.Text.StringBuilder("DNS seed discovery failed for every name:");

				foreach (KeyValuePair<string, string> entry in failures)
				{
					sb.Append(' ').Append(entry.Key).Append(": ").Append(entry.Value).Append(';');
				}

				if (sb.Length > 0 && sb[sb.Length - 1] == ';')
				{
					sb.Length--;
				}

				throw new AerospikeException(sb.ToString());
			}

			if (Log.WarnEnabled())
			{
				foreach (KeyValuePair<string, string> entry in failures)
				{
					Log.Warn("DNS seed name " + entry.Key + " failed: " + entry.Value);
				}
			}

			if (Log.DebugEnabled())
			{
				Log.Debug("DNS seed names " + string.Join(",", names) + " resolved to " + hosts.Count + " hosts");
			}

			return hosts.ToList();
		}

		/// <inheritdoc/>
		public bool NeedsPeriodicRefresh()
		{
			return true;
		}

		/// <inheritdoc/>
		public bool SupportsDeadline()
		{
			return true;
		}

		/// <summary>DNS names, in configured order.</summary>
		public IList<string> Names => names;

		/// <summary>Record type queried for each name.</summary>
		public RecordType Type => recordType;

		/// <summary>Port used for A and AAAA results.</summary>
		public int Port => port;

		/// <summary>TLS name set on every returned host. May be null.</summary>
		public string TlsName => tlsName;

		/// <summary>Total timeout in milliseconds for one refresh.</summary>
		public int Timeout => timeout;

		/// <summary>Configured resolver addresses. Empty when system resolvers are used.</summary>
		public IList<string> Servers => servers;

		/// <inheritdoc/>
		public override string ToString()
		{
			return "dns " + string.Join(",", names) + ' ' + recordType;
		}

		private List<Host> ResolveA(string name, long deadline)
		{
			List<string> addresses = Addresses(name, deadline, false);
			List<Host> list = new List<Host>(addresses.Count);

			foreach (string address in addresses)
			{
				list.Add(new Host(address, tlsName, port));
			}

			return list;
		}

		private List<Host> ResolveSrv(string name, long deadline)
		{
			List<Srv> records = new List<Srv>();

			foreach (string value in Query(name, DnsQuery.TypeSRV, deadline))
			{
				Srv srv = Srv.Parse(value);

				if (srv == null)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn("DNS seed name " + name + " returned an invalid SRV record: " + value);
					}
					continue;
				}

				if (!srv.target.Equals("."))
				{
					records.Add(srv);
				}
			}

			if (records.Count == 0)
			{
				return new List<Host>();
			}

			records.Sort(Srv.Comparer);

			Dictionary<string, string> targets = new Dictionary<string, string>();
			List<Host> list = new List<Host>(records.Count);

			foreach (Srv srv in records)
			{
				string address;

				if (!targets.TryGetValue(srv.target, out address))
				{
					try
					{
						List<string> resolved = Addresses(srv.target, deadline, true);

						if (resolved.Count == 0)
						{
							throw new AerospikeException("empty answer");
						}

						address = resolved[0];
					}
					catch (Exception e)
					{
						if (Log.WarnEnabled())
						{
							Log.Warn("DNS seed name " + name + " SRV target " + srv.target + " dropped: " + Reason(e));
						}

						address = null;
					}

					targets[srv.target] = address;
				}

				if (address != null)
				{
					list.Add(new Host(address, tlsName, srv.port));
				}
			}

			if (list.Count == 0)
			{
				throw new AerospikeException("no SRV target resolved");
			}

			return list;
		}

		private List<string> Addresses(string name, long deadline, bool first)
		{
			List<string> addresses = new List<string>();
			Exception error = null;
			ushort[] types = [DnsQuery.TypeA, DnsQuery.TypeAAAA];

			foreach (ushort type in types)
			{
				if (first && addresses.Count > 0)
				{
					break;
				}

				try
				{
					List<string> values = Query(name, type, deadline);
					values.Sort(StringComparer.Ordinal);
					addresses.AddRange(values);
				}
				catch (DnsQuery.DnsNxDomainException)
				{
					throw;
				}
				catch (Exception e)
				{
					error = e;
				}
			}

			if (addresses.Count == 0 && error != null)
			{
				throw error;
			}

			return addresses;
		}

		private List<string> Query(string name, ushort type, long deadline)
		{
			int remaining = RemainingMs(deadline);

			if (remaining <= 0)
			{
				throw new TimeoutException("timeout");
			}

			int perServer = Math.Max(1, remaining / Math.Max(1, resolvers.Count * 2));
			Exception last = null;

			foreach (IPEndPoint server in resolvers)
			{
				remaining = RemainingMs(deadline);

				if (remaining <= 0)
				{
					throw new TimeoutException("timeout");
				}

				try
				{
					return DnsQuery.Query(server, Absolute(name), type, Math.Min(perServer, remaining));
				}
				catch (Exception e)
				{
					last = e;
				}
			}

			throw last ?? new TimeoutException("timeout");
		}

		private static int RemainingMs(long deadlineTicks)
		{
			long remaining = deadlineTicks - Stopwatch.GetTimestamp();
			return (int)(remaining * 1000 / Stopwatch.Frequency);
		}

		private static string Absolute(string name)
		{
			return name.EndsWith(".") ? name : name + ".";
		}

		private static string Reason(Exception e)
		{
			if (e is DnsQuery.DnsNxDomainException)
			{
				return "NXDOMAIN";
			}

			if (e is DnsQuery.DnsServFailException)
			{
				return "SERVFAIL";
			}

			if (e is TimeoutException)
			{
				return "timeout";
			}

			return e.Message ?? e.GetType().Name;
		}

		private static IPEndPoint ParseServer(string server)
		{
			if (server == null)
			{
				throw new AerospikeException("Invalid DNS seed provider server: null");
			}

			string host;
			string portString = null;

			if (server.StartsWith("["))
			{
				int end = server.IndexOf(']');

				if (end < 0)
				{
					throw new AerospikeException("Invalid DNS seed provider server: '" + server + "'");
				}

				host = server.Substring(1, end - 1);

				if (end + 1 < server.Length)
				{
					if (server[end + 1] != ':')
					{
						throw new AerospikeException("Invalid DNS seed provider server: '" + server + "'");
					}

					portString = server.Substring(end + 2);
				}
			}
			else if (server.IndexOf(':') != server.LastIndexOf(':'))
			{
				host = server;
			}
			else
			{
				int index = server.IndexOf(':');

				if (index >= 0)
				{
					host = server.Substring(0, index);
					portString = server.Substring(index + 1);
				}
				else
				{
					host = server;
				}
			}

			if (!IPAddress.TryParse(host, out IPAddress address))
			{
				throw new AerospikeException("Invalid DNS seed provider server: '" + server + "': expected an IP address");
			}

			int serverPort = DnsPort;

			if (portString != null)
			{
				if (!int.TryParse(portString, out serverPort) || serverPort <= 0 || serverPort > 65535)
				{
					throw new AerospikeException("Invalid DNS seed provider server: '" + server + "': invalid port");
				}
			}

			return new IPEndPoint(address, serverPort);
		}

		private static List<IPEndPoint> SystemResolvers()
		{
			EndPointSet ends = new EndPointSet();

			foreach (NetworkInterface nic in NetworkInterface.GetAllNetworkInterfaces())
			{
				if (nic.OperationalStatus != OperationalStatus.Up)
				{
					continue;
				}

				IPInterfaceProperties props = nic.GetIPProperties();

				foreach (IPAddress dns in props.DnsAddresses)
				{
					if (dns.AddressFamily == AddressFamily.InterNetwork ||
						dns.AddressFamily == AddressFamily.InterNetworkV6)
					{
						ends.Add(new IPEndPoint(dns, DnsPort));
					}
				}
			}

			List<IPEndPoint> list = ends.ToList();

			if (list.Count == 0)
			{
				throw new AerospikeException("DNS seed provider: no system DNS server found. Set servers");
			}

			return list;
		}

		/// <summary>
		/// Builder for <see cref="DnsSeedCandidateProvider"/>. Values are validated by <see cref="Build"/>.
		/// </summary>
		public sealed class Builder
		{
			internal List<string> names;
			internal DnsSeedCandidateProvider.RecordType recordType = DnsSeedCandidateProvider.RecordType.A;
			internal int port = DefaultPort;
			internal string tlsName;
			internal int timeout = DefaultTimeout;
			internal List<string> servers;

			internal Builder()
			{
			}

			/// <summary>
			/// DNS names to resolve. Required: one or more names, no duplicates.
			/// </summary>
			public Builder Names(params string[] names)
			{
				this.names = (names != null) ? new List<string>(names) : null;
				return this;
			}

			/// <summary>
			/// DNS names to resolve.
			/// </summary>
			public Builder Names(IList<string> names)
			{
				this.names = (names != null) ? new List<string>(names) : null;
				return this;
			}

			/// <summary>
			/// Record type queried for each name. Default: <see cref="RecordType.A"/>.
			/// </summary>
			public Builder RecordType(DnsSeedCandidateProvider.RecordType recordType)
			{
				this.recordType = recordType;
				return this;
			}

			/// <summary>
			/// Port set on hosts resolved from A and AAAA records. Ignored for SRV.
			/// Default: <see cref="DefaultPort"/>.
			/// </summary>
			public Builder Port(int port)
			{
				this.port = port;
				return this;
			}

			/// <summary>
			/// TLS name set on every returned host. Null leaves defaulting to configured-seed rules.
			/// </summary>
			public Builder TlsName(string tlsName)
			{
				this.tlsName = tlsName;
				return this;
			}

			/// <summary>
			/// Total time in milliseconds for one refresh. Must be greater than zero.
			/// In tend mode must not exceed <see cref="ClientPolicy.discoveryTendRefreshDeadlineMs"/>.
			/// Default: <see cref="DefaultTimeout"/>.
			/// </summary>
			public Builder Timeout(int timeout)
			{
				this.timeout = timeout;
				return this;
			}

			/// <summary>
			/// Resolver addresses, each an IP with optional port. Null or empty uses system resolvers.
			/// </summary>
			public Builder Servers(params string[] servers)
			{
				this.servers = (servers != null) ? new List<string>(servers) : null;
				return this;
			}

			/// <summary>
			/// Resolver addresses.
			/// </summary>
			public Builder Servers(IList<string> servers)
			{
				this.servers = (servers != null) ? new List<string>(servers) : null;
				return this;
			}

			/// <summary>
			/// Validate the settings and build the provider.
			/// </summary>
			public DnsSeedCandidateProvider Build()
			{
				return new DnsSeedCandidateProvider(this);
			}
		}

		private sealed class Srv
		{
			internal static readonly Comparison<Srv> Comparer = (a, b) =>
			{
				int c = a.priority.CompareTo(b.priority);

				if (c != 0)
				{
					return c;
				}

				c = b.weight.CompareTo(a.weight);

				if (c != 0)
				{
					return c;
				}

				c = string.CompareOrdinal(a.target, b.target);
				return c != 0 ? c : a.port.CompareTo(b.port);
			};

			internal readonly int priority;
			internal readonly int weight;
			internal readonly int port;
			internal readonly string target;

			private Srv(int priority, int weight, int port, string target)
			{
				this.priority = priority;
				this.weight = weight;
				this.port = port;
				this.target = target;
			}

			internal static Srv Parse(string value)
			{
				string[] fields = value.Trim().Split((char[])null, StringSplitOptions.RemoveEmptyEntries);

				if (fields.Length != 4)
				{
					return null;
				}

				if (!int.TryParse(fields[0], out int priority) ||
					!int.TryParse(fields[1], out int weight) ||
					!int.TryParse(fields[2], out int port) ||
					port <= 0 || port > 65535)
				{
					return null;
				}

				return new Srv(priority, weight, port, fields[3].ToLowerInvariant());
			}
		}

		/// <summary>Insertion-ordered unique Host set (name+port).</summary>
		private sealed class HostSet
		{
			private readonly HashSet<Host> set = new HashSet<Host>();
			private readonly List<Host> list = new List<Host>();

			internal int Count => list.Count;

			internal void Add(Host host)
			{
				if (set.Add(host))
				{
					list.Add(host);
				}
			}

			internal List<Host> ToList() => new List<Host>(list);
		}

		/// <summary>Insertion-ordered unique IPEndPoint set.</summary>
		private sealed class EndPointSet
		{
			private readonly HashSet<IPEndPoint> set = new HashSet<IPEndPoint>();
			private readonly List<IPEndPoint> list = new List<IPEndPoint>();

			internal void Add(IPEndPoint end)
			{
				if (set.Add(end))
				{
					list.Add(end);
				}
			}

			internal List<IPEndPoint> ToList() => new List<IPEndPoint>(list);
		}
	}
}
