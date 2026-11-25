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
using System.Text;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// <see cref="AwsEc2TagSeedCandidateProvider"/> against real AWS, run by
	/// AerospikeTest/discovery/run_aws_tags_real.sh on an EC2 instance in the same region as a small
	/// self-managed Aerospike cluster whose nodes carry the tag. The instance profile role allows only
	/// <c>ec2:DescribeInstances</c>. Each latency run is a fresh process (<c>AwsTagsRealProbe</c>), so
	/// the cold call includes credential resolution through IMDSv2.
	/// <para>
	/// Required environment (set by the runner script):
	/// <list type="bullet">
	/// <item><c>AWS_TAGS_REAL_REGION</c>, <c>AWS_TAGS_REAL_TAG</c> (<c>key=value</c>)</item>
	/// <item><c>AWS_TAGS_REAL_EXPECTED_NODES</c>, <c>AWS_TAGS_REAL_EXCLUDED</c></item>
	/// <item><c>AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID</c>, <c>AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY</c>,
	/// <c>AWS_TAGS_REAL_DENIED_SESSION_TOKEN</c></item>
	/// </list>
	/// Optional: <c>AWS_TAGS_REAL_NAMESPACE</c>, <c>AWS_TAGS_REAL_RUNS</c>, <c>AWS_TAGS_REAL_WARM</c>,
	/// <c>AWS_TAGS_REAL_TEND_REFRESHES</c>, <c>AWS_TAGS_REAL_PROBE</c> (path to probe dll).
	/// </para>
	/// </summary>
	[TestClass]
	public class TestAwsTagsReal
	{
		private static readonly string[] AwsEnv =
		{
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
			"AWS_PROFILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
			"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"
		};

		private static readonly int RecommendedTimeout = AwsEc2TagSeedCandidateProvider.DefaultTimeout;
		private static readonly object MessageLock = new object();
		private static readonly List<string> Messages = new List<string>();

		private static string region;
		private static string tagKey;
		private static string tagValue;
		private static int expectedNodes;
		private static List<string> excluded;
		private static string ns;
		private static int runs;
		private static int warm;
		private static int tendRefreshes;
		private static string probeDll;
		private static string repoRoot;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AWS_TAGS_REAL_REGION")))
			{
				Assert.Inconclusive("AWS_TAGS_REAL_* not set. Run AerospikeTest/discovery/run_aws_tags_real.sh on EC2.");
			}

			region = RequiredEnv("AWS_TAGS_REAL_REGION");
			string tag = RequiredEnv("AWS_TAGS_REAL_TAG");
			int eq = tag.IndexOf('=');
			Assert.IsTrue(eq > 0 && eq < tag.Length - 1, "AWS_TAGS_REAL_TAG must be key=value, got: " + tag);
			tagKey = tag.Substring(0, eq);
			tagValue = tag.Substring(eq + 1);
			expectedNodes = int.Parse(RequiredEnv("AWS_TAGS_REAL_EXPECTED_NODES"));
			excluded = new List<string>(RequiredEnv("AWS_TAGS_REAL_EXCLUDED").Split(','));
			ns = OptionalEnv("AWS_TAGS_REAL_NAMESPACE", "test");
			runs = int.Parse(OptionalEnv("AWS_TAGS_REAL_RUNS", "20"));
			warm = int.Parse(OptionalEnv("AWS_TAGS_REAL_WARM", "20"));
			tendRefreshes = int.Parse(OptionalEnv("AWS_TAGS_REAL_TEND_REFRESHES", "30"));
			RequiredEnv("AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID");
			RequiredEnv("AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY");
			RequiredEnv("AWS_TAGS_REAL_DENIED_SESSION_TOKEN");

			foreach (string name in AwsEnv)
			{
				if (Environment.GetEnvironmentVariable(name) != null)
				{
					Assert.Fail(name + " is set; the instance profile must be the only credential source");
				}
			}

			repoRoot = FindRepoRoot();
			probeDll = Environment.GetEnvironmentVariable("AWS_TAGS_REAL_PROBE");
			if (string.IsNullOrEmpty(probeDll))
			{
				probeDll = Path.Combine(repoRoot, "AwsTagsRealProbe", "bin", "Release", "net8.0", "AwsTagsRealProbe.dll");
			}
			Assert.IsTrue(File.Exists(probeDll), "AwsTagsRealProbe dll not found: " + probeDll);

			Log.SetLevel(Log.Level.DEBUG);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN || level == Log.Level.ERROR ||
					message.StartsWith("AWS EC2 seed provider ", StringComparison.Ordinal))
				{
					lock (MessageLock)
					{
						Messages.Add(message);
					}
				}
			});
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
			Log.SetCallback(null);
		}

		[TestMethod]
		public void Case1LatencyAndImdsCredentials()
		{
			List<long> cold = new List<long>();
			List<long> warmTimes = new List<long>();
			int warmFailures = 0;

			for (int run = 1; run <= runs; run++)
			{
				List<string> lines = Probe(null);
				string credentials = Line(lines, "PROBE credentials ");
				string hosts = Line(lines, "PROBE hosts ");
				Console.WriteLine("AWS-REAL-RUN " + run + " credentials='" + credentials + "' hosts=" + hosts +
					" cold=" + Line(lines, "PROBE coldMicros ") + "us");

				Assert.IsNotNull(credentials, "no credentials line (AWS rejected the request?): " + Fmt(lines));
				Assert.IsTrue(credentials.Contains("credentials from imds"), credentials);
				AssertSeedList(hosts);
				cold.Add(long.Parse(Line(lines, "PROBE coldMicros ")));

				foreach (string micros in Line(lines, "PROBE warmMicros ").Replace("[", "").Replace("]", "").Replace(" ", "").Split(',', StringSplitOptions.RemoveEmptyEntries))
				{
					warmTimes.Add(long.Parse(micros));
				}
				warmFailures += int.Parse(Line(lines, "PROBE warmFailures "));
			}

			Console.WriteLine("AWS-REAL-LATENCY cold " + Stats(cold) + " runs=" + cold.Count);
			Console.WriteLine("AWS-REAL-LATENCY warm " + Stats(warmTimes) + " calls=" + warmTimes.Count + " failures=" +
				warmFailures);
			Console.WriteLine("AWS-REAL-LATENCY reference discoveryTendRefreshDeadline=100ms recommendedTimeout=" +
				RecommendedTimeout + "ms warmOverTimeout=" + CountOver(warmTimes, RecommendedTimeout) +
				" warmOverDeadline=" + CountOver(warmTimes, 100));
		}

		[TestMethod]
		public void Case2RoleWithoutPolicyRefused()
		{
			string secret = Environment.GetEnvironmentVariable("AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY");
			string token = Environment.GetEnvironmentVariable("AWS_TAGS_REAL_DENIED_SESSION_TOKEN");
			List<string> lines = Probe(new string[]
			{
				Environment.GetEnvironmentVariable("AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID"),
				secret,
				token
			});
			string error = Line(lines, "PROBE error ");
			string credentials = Line(lines, "PROBE credentials ");
			Console.WriteLine("AWS-REAL-DENIED credentials='" + credentials + "' error='" + error + "'");

			Assert.IsNotNull(error, "no error: " + Fmt(lines));
			Assert.IsTrue(error.Contains("UnauthorizedOperation") && error.Contains("(request id "), error);
			Assert.IsFalse(error.Contains(secret), error);
			Assert.IsFalse(error.Contains(token), error);
		}

		[TestMethod]
		public void Case3ClientJoinsEveryTaggedNode()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.discoveryRefreshIntervalMs = policy.tendInterval;
			policy.seedCandidateProvider = AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region(region)
				.TagFilter(tagKey, tagValue)
				.Build();

			// Empty Host[] hits the public seed ctor (not the InternalsVisibleTo ClientPolicy-only helper).
			AerospikeClient client = new AerospikeClient(policy, Array.Empty<Host>());
			try
			{
				long deadline = Environment.TickCount64 + 60000;

				while (client.Nodes.Length < expectedNodes || OwnedPartitions(client) < Node.PARTITIONS)
				{
					if (Environment.TickCount64 > deadline)
					{
						Assert.Fail("nodes=" + client.Nodes.Length + " owned=" + OwnedPartitions(client) + " " +
							Fmt(SnapshotMessages()));
					}
					Thread.Sleep(100);
				}

				lock (MessageLock)
				{
					Messages.Clear();
				}

				long wait = (long)tendRefreshes * policy.discoveryRefreshIntervalMs + 60000;
				deadline = Environment.TickCount64 + wait;

				while (CountContains(" returned ") < tendRefreshes && CountContains("Seed candidate provider exceeded") == 0)
				{
					if (Environment.TickCount64 > deadline)
					{
						Assert.Fail("only " + CountContains(" returned ") + " refreshes in " + wait + "ms: " +
							Fmt(SnapshotMessages()));
					}
					Thread.Sleep(100);
				}

				Console.WriteLine("AWS-REAL-JOIN nodes=" + client.Nodes.Length + " expected=" + expectedNodes +
					" ownedPartitions=" + OwnedPartitions(client) + '/' + Node.PARTITIONS + " tendRefreshes=" +
					CountContains(" returned ") + " overruns=" + CountContains("exceeding tend deadline") +
					" failures=" + CountContains(" failed: ") + " tripped=" +
					(CountContains("Seed candidate provider exceeded") > 0));
				Assert.AreEqual(expectedNodes, client.Nodes.Length);
				Assert.AreEqual(0, CountContains("Seed candidate provider exceeded"));
			}
			finally
			{
				client.Close();
			}
		}

		private static void AssertSeedList(string hosts)
		{
			Assert.IsNotNull(hosts);

			foreach (string address in excluded)
			{
				Assert.IsFalse(hosts.Contains(address.Trim() + ' '), address + " in " + hosts);
			}
			int n = hosts.Equals("[]", StringComparison.Ordinal) ? 0 : hosts.Split(',').Length;
			Assert.AreEqual(expectedNodes, n, hosts);
		}

		private static List<string> Probe(string[] credentials)
		{
			ProcessStartInfo psi = new ProcessStartInfo
			{
				FileName = "dotnet",
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				UseShellExecute = false,
				CreateNoWindow = true,
				WorkingDirectory = repoRoot
			};
			psi.ArgumentList.Add("exec");
			psi.ArgumentList.Add(probeDll);
			psi.ArgumentList.Add(region);
			psi.ArgumentList.Add(tagKey);
			psi.ArgumentList.Add(tagValue);
			psi.ArgumentList.Add(warm.ToString());

			if (credentials != null)
			{
				psi.Environment["AWS_ACCESS_KEY_ID"] = credentials[0];
				psi.Environment["AWS_SECRET_ACCESS_KEY"] = credentials[1];
				psi.Environment["AWS_SESSION_TOKEN"] = credentials[2];
			}

			using (Process process = Process.Start(psi))
			{
				Assert.IsNotNull(process, "failed to start AwsTagsRealProbe");
				string output = process.StandardOutput.ReadToEnd();
				string err = process.StandardError.ReadToEnd();
				if (!process.WaitForExit(120000))
				{
					try { process.Kill(true); } catch { }
					Assert.Fail("probe did not exit: " + output + err);
				}
				return new List<string>((output + err).Split('\n'));
			}
		}

		private static string Line(List<string> lines, string prefix)
		{
			foreach (string line in lines)
			{
				string trimmed = line.TrimEnd('\r');
				if (trimmed.StartsWith(prefix, StringComparison.Ordinal))
				{
					return trimmed.Substring(prefix.Length).Trim();
				}
			}
			return null;
		}

		private static string Stats(List<long> micros)
		{
			List<long> sorted = new List<long>(micros);
			sorted.Sort();
			return string.Format(System.Globalization.CultureInfo.InvariantCulture,
				"min={0:0.0}ms median={1:0.0}ms p99={2:0.0}ms max={3:0.0}ms",
				sorted[0] / 1000.0,
				sorted[sorted.Count / 2] / 1000.0,
				sorted[Math.Min(sorted.Count - 1, (int)Math.Ceiling(sorted.Count * 0.99) - 1)] / 1000.0,
				sorted[sorted.Count - 1] / 1000.0);
		}

		private static int CountOver(List<long> micros, int ms)
		{
			int n = 0;
			foreach (long m in micros)
			{
				if (m > ms * 1000L)
				{
					n++;
				}
			}
			return n;
		}

		private static int CountContains(string text)
		{
			lock (MessageLock)
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
		}

		private static List<string> SnapshotMessages()
		{
			lock (MessageLock)
			{
				return new List<string>(Messages);
			}
		}

		private static int OwnedPartitions(AerospikeClient client)
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

		private static string RequiredEnv(string name)
		{
			string value = Environment.GetEnvironmentVariable(name);
			if (string.IsNullOrEmpty(value))
			{
				throw new InvalidOperationException("Environment variable " + name +
					" is not set. Run AerospikeTest/discovery/run_aws_tags_real.sh");
			}
			return value;
		}

		private static string OptionalEnv(string name, string fallback)
		{
			string value = Environment.GetEnvironmentVariable(name);
			return string.IsNullOrEmpty(value) ? fallback : value;
		}

		private static string FindRepoRoot()
		{
			string dir = AppContext.BaseDirectory;
			while (dir != null)
			{
				if (File.Exists(Path.Combine(dir, "Aerospike.sln")) &&
					Directory.Exists(Path.Combine(dir, "AwsTagsRealProbe")))
				{
					return dir;
				}
				dir = Directory.GetParent(dir)?.FullName;
			}
			throw new InvalidOperationException("Could not find repo root from " + AppContext.BaseDirectory);
		}

		private static string Fmt(IEnumerable<string> values)
		{
			StringBuilder sb = new StringBuilder("[");
			foreach (string value in values)
			{
				if (sb.Length > 1)
				{
					sb.Append(", ");
				}
				sb.Append(value);
			}
			return sb.Append(']').ToString();
		}
	}
}
