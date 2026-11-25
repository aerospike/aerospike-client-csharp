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
using System.Diagnostics;
using System.Text;
using Aerospike.Client;

namespace Aerospike.AwsTagsRealProbe
{
	/// <summary>
	/// Probe for the aws-tags real AWS harness in aerospike-client-conformance (aws-real/).
	/// Modes <c>latency</c>, <c>denied</c>, and <c>join</c> each print one JSON object on stdout.
	/// </summary>
	public static class Program
	{
		private static readonly ConcurrentQueue<string> Messages = new ConcurrentQueue<string>();

		public static int Main(string[] args)
		{
			// Legacy: region tagKey tagValue warmCount — used by TestAwsTagsReal / run_aws_tags_real.sh.
			if (args.Length >= 4)
			{
				return LegacyProbe(args);
			}

			if (args.Length != 1)
			{
				Console.Error.WriteLine("usage: AwsTagsRealProbe latency|denied|join");
				return 2;
			}

			string tag = Env("AWS_REAL_TAG", null);
			int eq = tag.IndexOf('=');

			if (eq <= 0 || eq == tag.Length - 1)
			{
				Console.Error.WriteLine("AWS_REAL_TAG must be key=value");
				return 2;
			}

			string region = Env("AWS_REAL_REGION", null);
			string tagKey = tag.Substring(0, eq);
			string tagValue = tag.Substring(eq + 1);

			switch (args[0])
			{
				case "latency":
					Log.SetLevel(Log.Level.INFO);
					Log.SetCallback((_, message) => Messages.Enqueue(message));
					Console.WriteLine(Latency(Provider(region, tagKey, tagValue),
						int.Parse(Env("AWS_REAL_WARM", "20"))));
					break;

				case "denied":
					Log.SetLevel(Log.Level.INFO);
					Log.SetCallback((_, message) => Messages.Enqueue(message));
					Console.WriteLine(Denied(Provider(region, tagKey, tagValue)));
					break;

				case "join":
					Log.SetLevel(Log.Level.DEBUG);
					Log.SetCallback((level, message) =>
					{
						if (level == Log.Level.WARN || level == Log.Level.ERROR ||
							message.StartsWith("AWS EC2 seed provider ", StringComparison.Ordinal))
						{
							Messages.Enqueue(message);
						}
					});
					Console.WriteLine(Join(region, tagKey, tagValue,
						int.Parse(Env("AWS_REAL_EXPECTED_NODES", null)),
						Env("AWS_REAL_NAMESPACE", "test"),
						int.Parse(Env("AWS_REAL_REFRESHES", "30"))));
					break;

				default:
					Console.Error.WriteLine("unknown mode " + args[0]);
					return 2;
			}

			return 0;
		}

		/// <summary>
		/// Fresh-process latency probe for <c>TestAwsTagsReal</c> (PROBE lines, not JSON).
		/// </summary>
		private static int LegacyProbe(string[] args)
		{
			ConcurrentQueue<string> messages = new ConcurrentQueue<string>();
			Log.SetLevel(Log.Level.INFO);
			Log.SetCallback((level, message) => messages.Enqueue(level + " " + message));

			AwsEc2TagSeedCandidateProvider provider = Provider(args[0], args[1], args[2]);
			int warm = int.Parse(args[3]);
			Host[] none = Array.Empty<Host>();

			try
			{
				long begin = NanoTime();
				List<Host> hosts = provider.RefreshSeedCandidates(new SeedCandidateContext(null, none, 0));
				long cold = (NanoTime() - begin) / 1000L;
				List<long> warmTimes = new List<long>();
				int warmFailures = 0;

				for (int i = 1; i <= warm; i++)
				{
					begin = NanoTime();

					try
					{
						provider.RefreshSeedCandidates(new SeedCandidateContext(null, none, i));
					}
					catch (AerospikeException ae)
					{
						warmFailures++;
						Console.WriteLine("PROBE warmFailure " + ae.Message.Replace('\n', ' '));
					}
					warmTimes.Add((NanoTime() - begin) / 1000L);
				}

				foreach (string m in messages)
				{
					if (m.Contains("credentials from"))
					{
						Console.WriteLine("PROBE credentials " + m);
					}
				}
				Console.WriteLine("PROBE coldMicros " + cold);
				Console.WriteLine("PROBE warmMicros [" + string.Join(", ", warmTimes) + "]");
				Console.WriteLine("PROBE warmFailures " + warmFailures);
				Console.WriteLine("PROBE hosts [" + string.Join(", ", hosts) + "]");
				return 0;
			}
			catch (AerospikeException ae)
			{
				Console.WriteLine("PROBE error " + ae.Message.Replace('\n', ' '));
				return 0;
			}
		}

		private static AwsEc2TagSeedCandidateProvider Provider(string region, string tagKey, string tagValue)
		{
			return AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region(region)
				.TagFilter(tagKey, tagValue)
				.Build();
		}

		private static string Latency(AwsEc2TagSeedCandidateProvider provider, int warm)
		{
			Json json = new Json().Field("mode", "latency");
			Host[] none = Array.Empty<Host>();
			long begin = NanoTime();
			List<Host> hosts;

			try
			{
				hosts = provider.RefreshSeedCandidates(new SeedCandidateContext(null, none, 0));
			}
			catch (AerospikeException ae)
			{
				return json.Field("credentials_source", CredentialSource()).Field("error", Message(ae)).End();
			}

			long cold = (NanoTime() - begin) / 1000L;
			List<long> warmTimes = new List<long>();
			List<string> warmFailures = new List<string>();

			for (int i = 1; i <= warm; i++)
			{
				begin = NanoTime();

				try
				{
					provider.RefreshSeedCandidates(new SeedCandidateContext(null, none, i));
				}
				catch (AerospikeException ae)
				{
					warmFailures.Add(Message(ae));
				}
				warmTimes.Add((NanoTime() - begin) / 1000L);
			}

			return json.Field("credentials_source", CredentialSource())
				.Strings("hosts", HostStrings(hosts))
				.Field("cold_us", cold)
				.Longs("warm_us", warmTimes)
				.Strings("warm_failures", warmFailures)
				.End();
		}

		private static string Denied(AwsEc2TagSeedCandidateProvider provider)
		{
			Json json = new Json().Field("mode", "denied");

			try
			{
				List<Host> hosts = provider.RefreshSeedCandidates(
					new SeedCandidateContext(null, Array.Empty<Host>(), 0));
				return json.Field("credentials_source", CredentialSource())
					.Strings("hosts", HostStrings(hosts))
					.End();
			}
			catch (AerospikeException ae)
			{
				return json.Field("credentials_source", CredentialSource()).Field("error", Message(ae)).End();
			}
		}

		private static string Join(string region, string tagKey, string tagValue, int expectedNodes,
			string ns, int refreshes)
		{
			ClientPolicy policy = new ClientPolicy();
			policy.discoveryRefreshIntervalMs = policy.tendInterval;
			policy.seedCandidateProvider = Provider(region, tagKey, tagValue);
			Json json = new Json().Field("mode", "join");

			try
			{
				using (AerospikeClient client = new AerospikeClient(policy, Array.Empty<Host>()))
				{
					long deadline = Environment.TickCount64 + 60000;

					while (client.Nodes.Length < expectedNodes || OwnedPartitions(client, ns) < Node.PARTITIONS)
					{
						if (Environment.TickCount64 > deadline)
						{
							return Counts(json, client, ns).Field("error",
								"nodes=" + client.Nodes.Length + " owned=" + OwnedPartitions(client, ns) +
								" after 60s").End();
						}
						Thread.Sleep(100);
					}

					while (!Messages.IsEmpty)
					{
						Messages.TryDequeue(out _);
					}

					long wait = (long)refreshes * policy.discoveryRefreshIntervalMs + 60000;
					deadline = Environment.TickCount64 + wait;

					while (Refreshes() < refreshes && Count("Seed candidate provider exceeded") == 0)
					{
						if (Environment.TickCount64 > deadline)
						{
							return Counts(json, client, ns).Field("error",
								"only " + Refreshes() + " refreshes in " + wait + "ms").End();
						}
						Thread.Sleep(100);
					}

					return Counts(json, client, ns).End();
				}
			}
			catch (AerospikeException ae)
			{
				return json.Field("error", Message(ae)).End();
			}
		}

		private static Json Counts(Json json, AerospikeClient client, string ns)
		{
			List<string> failures = new List<string>();

			foreach (string m in Messages)
			{
				if (m.Contains("Seed candidate refresh failed"))
				{
					failures.Add(m);
				}
			}

			return json.Field("nodes", client.Nodes.Length)
				.Field("owned_partitions", OwnedPartitions(client, ns))
				.Field("partitions", Node.PARTITIONS)
				.Field("refreshes", Refreshes())
				.Field("overruns", Count("exceeding tend deadline"))
				.Strings("failures", failures)
				.Field("tripped", Count("Seed candidate provider exceeded") > 0);
		}

		private static string CredentialSource()
		{
			const string prefix = "AWS EC2 seed provider credentials from ";

			foreach (string m in Messages)
			{
				if (m.StartsWith(prefix, StringComparison.Ordinal))
				{
					return m.Substring(prefix.Length).Trim();
				}
			}

			return null;
		}

		private static int Refreshes()
		{
			int n = 0;

			foreach (string m in Messages)
			{
				if (m.StartsWith("AWS EC2 seed provider ", StringComparison.Ordinal) && m.Contains(" returned "))
				{
					n++;
				}
			}

			return n;
		}

		private static int Count(string text)
		{
			int n = 0;

			foreach (string m in Messages)
			{
				if (m.Contains(text))
				{
					n++;
				}
			}

			return n;
		}

		private static int OwnedPartitions(AerospikeClient client, string ns)
		{
			// partitionMap is internal; use reflection so the probe need not be a signed friend.
			System.Reflection.FieldInfo field = typeof(Cluster).GetField("partitionMap",
				System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic |
				System.Reflection.BindingFlags.Public);

			if (field == null)
			{
				return 0;
			}

			object mapObj = field.GetValue(client.Cluster);

			if (mapObj is not System.Collections.IDictionary map || !map.Contains(ns))
			{
				return 0;
			}

			object partitions = map[ns];
			System.Reflection.FieldInfo replicasField = partitions.GetType().GetField("replicas",
				System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic |
				System.Reflection.BindingFlags.Public);

			if (replicasField?.GetValue(partitions) is not Node[][] replicas || replicas.Length == 0)
			{
				return 0;
			}

			int owned = 0;

			for (int i = 0; i < Node.PARTITIONS && i < replicas[0].Length; i++)
			{
				Node node = replicas[0][i];

				if (node != null && node.Active)
				{
					owned++;
				}
			}

			return owned;
		}

		private static List<string> HostStrings(List<Host> hosts)
		{
			List<string> list = new List<string>(hosts.Count);

			foreach (Host host in hosts)
			{
				list.Add(host.name + ':' + host.port);
			}

			return list;
		}

		private static long NanoTime()
		{
			return (long)(Stopwatch.GetTimestamp() * (1000000000.0 / Stopwatch.Frequency));
		}

		private static string Message(AerospikeException ae)
		{
			return ae.Message.Replace('\n', ' ');
		}

		private static string Env(string name, string defaultValue)
		{
			string value = Environment.GetEnvironmentVariable(name);

			if (string.IsNullOrEmpty(value))
			{
				if (defaultValue == null)
				{
					Console.Error.WriteLine(name + " is not set");
					Environment.Exit(2);
				}

				return defaultValue;
			}

			return value;
		}

		private sealed class Json
		{
			private readonly StringBuilder sb = new StringBuilder("{");

			public Json Field(string name, object value)
			{
				Key(name);

				if (value == null)
				{
					sb.Append("null");
				}
				else if (value is bool || value is byte || value is sbyte || value is short || value is ushort ||
					value is int || value is uint || value is long || value is ulong || value is float || value is double)
				{
					if (value is bool b)
					{
						sb.Append(b ? "true" : "false");
					}
					else
					{
						sb.Append(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
					}
				}
				else
				{
					Quote(value.ToString());
				}

				return this;
			}

			public Json Strings(string name, List<string> values)
			{
				Key(name);
				sb.Append('[');

				for (int i = 0; i < values.Count; i++)
				{
					if (i > 0)
					{
						sb.Append(',');
					}

					Quote(values[i]);
				}

				sb.Append(']');
				return this;
			}

			public Json Longs(string name, List<long> values)
			{
				Key(name);
				sb.Append('[');

				for (int i = 0; i < values.Count; i++)
				{
					if (i > 0)
					{
						sb.Append(',');
					}

					sb.Append(values[i]);
				}

				sb.Append(']');
				return this;
			}

			public string End()
			{
				return sb.Append('}').ToString();
			}

			private void Key(string name)
			{
				if (sb.Length > 1)
				{
					sb.Append(',');
				}

				Quote(name);
				sb.Append(':');
			}

			private void Quote(string s)
			{
				sb.Append('"');

				foreach (char c in s)
				{
					switch (c)
					{
						case '"':
							sb.Append("\\\"");
							break;
						case '\\':
							sb.Append("\\\\");
							break;
						case '\n':
							sb.Append("\\n");
							break;
						case '\r':
							sb.Append("\\r");
							break;
						case '\t':
							sb.Append("\\t");
							break;
						default:
							if (c < 0x20)
							{
								sb.AppendFormat("\\u{0:x4}", (int)c);
							}
							else
							{
								sb.Append(c);
							}
							break;
					}
				}

				sb.Append('"');
			}
		}
	}
}
