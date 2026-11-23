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
using System.Collections;
using System.Diagnostics;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Values of an aerospike-client-conformance test environment (<c>envs/&lt;environment&gt;.sh up</c>),
	/// read from the environment variables its values file sets, and its control verbs, run as
	/// <c>"$ENV_CTL" &lt;verb&gt;</c>. A run without the variables is inconclusive so unit test runs skip.
	/// </summary>
	internal static class ConformanceEnv
	{
		internal const string ConfigUrlVariable = "AEROSPIKE_CLIENT_CONFIG_URL";

		private const int ControlWaitSeconds = 300;

		/// <summary>
		/// Mark the calling test class inconclusive when any of the keys is not set or empty.
		/// </summary>
		internal static void Require(params string[] keys)
		{
			List<string> missing = new List<string>();

			foreach (string key in keys)
			{
				if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key)))
				{
					missing.Add(key);
				}
			}

			if (missing.Count > 0)
			{
				Assert.Inconclusive("Conformance environment not set: " + string.Join(", ", missing) +
					" missing. Bring the environment up with envs/<environment>.sh up and load its values file");
			}
		}

		/// <summary>
		/// Return the value of a key, failing when it is not set or empty.
		/// </summary>
		internal static string Get(string key)
		{
			string value = Environment.GetEnvironmentVariable(key);

			if (string.IsNullOrEmpty(value))
			{
				throw new InvalidOperationException("Environment variable " + key + " is not set. Bring the " +
					"conformance environment up with envs/<environment>.sh up and load its values file");
			}
			return value;
		}

		/// <summary>
		/// Return the integer value of a key, failing when it is not set.
		/// </summary>
		internal static int GetInt(string key)
		{
			return int.Parse(Get(key));
		}

		/// <summary>
		/// Fail when a config file URL is already set, because it would apply to every client the test
		/// class constructs.
		/// </summary>
		internal static void RequireNoConfig()
		{
			if (Environment.GetEnvironmentVariable(ConfigUrlVariable) != null)
			{
				throw new InvalidOperationException(ConfigUrlVariable + " is set and would override the test config");
			}
		}

		/// <summary>
		/// Point the client at a YAML config file by path. The values file of an environment holds a
		/// path, while the client reads a file URI from <c>AEROSPIKE_CLIENT_CONFIG_URL</c>.
		/// </summary>
		internal static void SetConfig(string path)
		{
			Environment.SetEnvironmentVariable(ConfigUrlVariable, new Uri(Path.GetFullPath(path)).AbsoluteUri);
		}

		/// <summary>
		/// Run a verb of the environment's script and return its standard output, failing when it does
		/// not exit with 0.
		/// </summary>
		internal static string Control(params string[] verb)
		{
			string command = Get("ENV_CTL");
			ProcessStartInfo psi = new ProcessStartInfo
			{
				FileName = command,
				RedirectStandardOutput = true,
				UseShellExecute = false,
				CreateNoWindow = true
			};

			foreach (string argument in verb)
			{
				psi.ArgumentList.Add(argument);
			}

			string description = command + ' ' + string.Join(" ", verb);

			using (Process process = Process.Start(psi))
			{
				if (process == null)
				{
					throw new InvalidOperationException("Failed to start " + description);
				}

				Task<string> output = process.StandardOutput.ReadToEndAsync();

				if (!process.WaitForExit(ControlWaitSeconds * 1000))
				{
					try
					{
						process.Kill(true);
					}
					catch (Exception)
					{
						// The process may have exited between the wait and the kill.
					}
					throw new InvalidOperationException(description + " did not finish within " + ControlWaitSeconds + "s");
				}

				string text = output.GetAwaiter().GetResult().Trim();

				if (process.ExitCode != 0)
				{
					throw new InvalidOperationException(description + " exited with " + process.ExitCode + ": " + text);
				}
				return text;
			}
		}

		internal static List<string> NodeNames(AerospikeClient client)
		{
			List<string> names = new List<string>();

			foreach (Node node in client.Nodes)
			{
				names.Add(node.Name);
			}
			names.Sort(StringComparer.Ordinal);
			return names;
		}

		internal static int OwnedPartitions(AerospikeClient client, string ns)
		{
			if (!client.Cluster.partitionMap.TryGetValue(ns, out Partitions partitions) || partitions == null)
			{
				return 0;
			}

			int owned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = partitions.replicas.Length > 0 ? partitions.replicas[0][i] : null;

				if (node != null && node.Active)
				{
					owned++;
				}
			}
			return owned;
		}

		/// <summary>
		/// Return the number of master partitions without an active node in the client's partition map
		/// for the namespace, and fill the owners with the number of partitions each node owns.
		/// </summary>
		internal static int Owners(AerospikeClient client, string ns, SortedDictionary<string, int> owners)
		{
			owners.Clear();

			if (!client.Cluster.partitionMap.TryGetValue(ns, out Partitions partitions) || partitions == null ||
				partitions.replicas.Length == 0)
			{
				return Node.PARTITIONS;
			}

			Node[] masters = partitions.replicas[0];
			int unowned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = masters[i];

				if (node == null || !node.Active)
				{
					unowned++;
				}
				else
				{
					owners[node.Name] = owners.GetValueOrDefault(node.Name, 0) + 1;
				}
			}
			return unowned;
		}

		/// <summary>
		/// Return the node name to published address map of the client's nodes, sorted by name.
		/// </summary>
		internal static SortedDictionary<string, string> NodeHosts(AerospikeClient client)
		{
			SortedDictionary<string, string> hosts = new SortedDictionary<string, string>(StringComparer.Ordinal);

			foreach (Node node in client.Nodes)
			{
				hosts[node.Name] = node.Host.name + ':' + node.Host.port;
			}
			return hosts;
		}

		internal static string Fmt(IEnumerable values)
		{
			StringBuilder sb = new StringBuilder("[");

			foreach (object value in values)
			{
				if (sb.Length > 1)
				{
					sb.Append(", ");
				}
				sb.Append(value);
			}
			return sb.Append(']').ToString();
		}

		internal static string FmtMap<K, V>(IDictionary<K, V> map)
		{
			StringBuilder sb = new StringBuilder("{");

			foreach (KeyValuePair<K, V> entry in map)
			{
				if (sb.Length > 1)
				{
					sb.Append(", ");
				}
				sb.Append(entry.Key).Append('=').Append(entry.Value);
			}
			return sb.Append('}').ToString();
		}

		/// <summary>
		/// Thread safe list of captured log messages.
		/// </summary>
		internal sealed class MessageList
		{
			private readonly object sync = new object();
			private readonly List<string> list = new List<string>();

			internal void Add(string message)
			{
				lock (sync)
				{
					list.Add(message);
				}
			}

			internal void Clear()
			{
				lock (sync)
				{
					list.Clear();
				}
			}

			internal int Count
			{
				get
				{
					lock (sync)
					{
						return list.Count;
					}
				}
			}

			internal List<string> Snapshot()
			{
				lock (sync)
				{
					return new List<string>(list);
				}
			}

			internal List<string> Containing(string text)
			{
				List<string> matches = new List<string>();

				foreach (string message in Snapshot())
				{
					if (message.Contains(text))
					{
						matches.Add(message);
					}
				}
				return matches;
			}
		}
	}
}
