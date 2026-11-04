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
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// L5 environment the aerospike-client-conformance l5-aws harness gives a row
	/// (l5-aws/README.md there): an AerospikeCluster on EKS reached only through a PrivateLink
	/// endpoint name, and <c>l5ctl</c>, which drives the disruptions. Every <c>l5ctl</c> call is
	/// bounded, and the <c>L5-*</c> lines it prints are parsed into <see cref="Line"/>s.
	/// </summary>
	internal sealed class L5Environment
	{
		public const string Namespace = "test";
		public const string User = "admin";
		public const int ClusterSize = 3;
		public const int TendInterval = 250;
		public const int Timeout = 1000;
		public const long CtlTimeoutMs = 300000;
		public const int ReadyTimeoutS = 1200;
		public const int HealthyTimeoutS = 600;
		public const long CtlMarginMs = 120000;

		public static readonly string[] Required =
		{
			"L5_CONTROL", "L5_ENDPOINT_NAME", "L5_SEED_PORT", "L5_CTL",
			"DISCOVERY_KUBECONFIG", "DISCOVERY_ENDPOINT_HOSTNAME", "AEROSPIKE_ADMIN_PASSWORD"
		};

		public string Control { get; }
		public string EndpointName { get; }
		public int SeedPort { get; }
		/// <summary>Path to the <c>l5ctl</c> binary (<c>L5_CTL</c>).</summary>
		public string CtlPath { get; }
		public string RunnerAz { get; }
		private readonly string password;

		/// <summary>
		/// One <c>L5-*</c> line: its tag and its <c>key=value</c> fields.
		/// </summary>
		public sealed class Line
		{
			public string Tag { get; }
			public IReadOnlyDictionary<string, string> Fields { get; }
			public string Text { get; }

			public Line(string text)
			{
				string[] tokens = text.Trim().Split((char[])null, StringSplitOptions.RemoveEmptyEntries);
				Dictionary<string, string> map = new Dictionary<string, string>(StringComparer.Ordinal);

				for (int i = 1; i < tokens.Length; i++)
				{
					int index = tokens[i].IndexOf('=');

					if (index > 0)
					{
						map[tokens[i].Substring(0, index)] = tokens[i].Substring(index + 1);
					}
				}

				Tag = tokens[0];
				Fields = new ReadOnlyDictionary<string, string>(map);
				Text = text.Trim();
			}

			public string Get(string key)
			{
				Assert.IsTrue(Fields.TryGetValue(key, out string value), "No " + key + " in " + Text);
				return value;
			}

			public override string ToString() => Text;
		}

		/// <summary>
		/// Outcome of one <c>l5ctl</c> call.
		/// </summary>
		public sealed class Result
		{
			public string Command { get; }
			public int Exit { get; }
			public long ElapsedMs { get; }
			public IReadOnlyList<Line> Lines { get; }
			public string Stderr { get; }

			public Result(string command, int exit, long elapsedMs, IReadOnlyList<Line> lines, string stderr)
			{
				Command = command;
				Exit = exit;
				ElapsedMs = elapsedMs;
				Lines = lines;
				Stderr = stderr;
			}

			public List<Line> Tagged(string tag)
			{
				List<Line> list = new List<Line>();

				foreach (Line line in Lines)
				{
					if (line.Tag.Equals(tag, StringComparison.Ordinal))
					{
						list.Add(line);
					}
				}

				return list;
			}

			public Line First(string tag)
			{
				List<Line> list = Tagged(tag);
				Assert.IsTrue(list.Count > 0, "No " + tag + " line from " + Command + ": " + Format(Lines));
				return list[0];
			}

			public Result AssertSucceeded()
			{
				Assert.AreEqual(0, Exit, Command + " exited " + Exit + ": " + Stderr);
				return this;
			}
		}

		private L5Environment()
		{
			Control = Environment.GetEnvironmentVariable("L5_CONTROL");
			EndpointName = Environment.GetEnvironmentVariable("L5_ENDPOINT_NAME");
			SeedPort = int.Parse(Environment.GetEnvironmentVariable("L5_SEED_PORT"));
			CtlPath = Environment.GetEnvironmentVariable("L5_CTL");
			RunnerAz = Optional("L5_RUNNER_AZ");
			password = Environment.GetEnvironmentVariable("AEROSPIKE_ADMIN_PASSWORD");
		}

		/// <summary>
		/// Return the environment. When the harness has not set the required variables, the test is
		/// inconclusive so ordinary <c>dotnet test</c> stays green. When some but not all are set,
		/// fail naming every missing variable.
		/// </summary>
		public static L5Environment Require(params string[] extra)
		{
			List<string> names = new List<string>(Required);
			names.AddRange(extra);
			List<string> missing = new List<string>();

			foreach (string name in names)
			{
				if (string.IsNullOrWhiteSpace(Optional(name)))
				{
					missing.Add(name);
				}
			}

			if (missing.Count == names.Count)
			{
				Assert.Inconclusive("L5 needs the aerospike-client-conformance l5-aws harness " +
					"(AerospikeTest/discovery/l5-aws.manifest via l5-aws/run.sh), which sets " +
					string.Join(", ", Required));
			}

			if (missing.Count > 0)
			{
				Console.WriteLine("L5-ENVIRONMENT missing=" + string.Join(",", missing));
				Assert.Fail("L5 environment missing " + string.Join(", ", missing) +
					": run the row through aerospike-client-conformance l5-aws/run.sh with " +
					"AerospikeTest/discovery/l5-aws.manifest, which sets them");
			}

			string control = Environment.GetEnvironmentVariable("L5_CONTROL");
			Assert.IsTrue("none".Equals(control) || "resolvable".Equals(control),
				"L5_CONTROL must be none or resolvable, got " + control);
			string seedPort = Environment.GetEnvironmentVariable("L5_SEED_PORT");
			Assert.IsTrue(System.Text.RegularExpressions.Regex.IsMatch(seedPort, "^[1-9][0-9]{0,4}$"),
				"L5_SEED_PORT is not a port: " + seedPort);
			string ctl = Environment.GetEnvironmentVariable("L5_CTL");
			Assert.IsTrue(File.Exists(ctl), "L5_CTL " + ctl + " does not exist");
			Assert.AreEqual(Environment.GetEnvironmentVariable("L5_ENDPOINT_NAME"),
				Environment.GetEnvironmentVariable("DISCOVERY_ENDPOINT_HOSTNAME"),
				"DISCOVERY_ENDPOINT_HOSTNAME");

			L5Environment env = new L5Environment();
			Console.WriteLine("L5-ENVIRONMENT control=" + env.Control + " endpoint=" + env.EndpointName +
				" seedPort=" + env.SeedPort + " ctl=" + env.CtlPath + " runnerAz=" + env.RunnerAz);
			return env;
		}

		/// <summary>
		/// Client policy of the L2 discovery tests, with the cluster's admin credentials.
		/// </summary>
		public ClientPolicy Policy()
		{
			return new ClientPolicy
			{
				user = User,
				password = password,
				tendInterval = TendInterval,
				timeout = Timeout
			};
		}

		public Host Seed() => new Host(EndpointName, SeedPort);

		public string Advertised(int nodePort) => EndpointName + ':' + nodePort;

		public void WaitReady()
		{
			Ctl(ReadyTimeoutS * 1000L + CtlMarginMs, "wait-ready", "--timeout", ReadyTimeoutS.ToString())
				.AssertSucceeded();
		}

		public void WaitHealthy()
		{
			Ctl(HealthyTimeoutS * 1000L + CtlMarginMs, "wait-healthy", "--timeout", HealthyTimeoutS.ToString())
				.AssertSucceeded();
		}

		/// <summary>
		/// Set the AerospikeCluster back to <paramref name="size"/> and wait for it when it is not
		/// at that size.
		/// </summary>
		public void RestoreSize(K8sCluster cluster, int size)
		{
			if (cluster.Size() != size || cluster.Pods().Count != size)
			{
				Ctl(CtlTimeoutMs, "scale", size.ToString()).AssertSucceeded();
				WaitReady();
			}
		}

		/// <summary>
		/// Run <c>$L5_CTL args</c>, print its <c>L5-*</c> lines and return its exit code. Fails the
		/// test when it does not finish within <paramref name="boundMs"/>.
		/// </summary>
		public Result Ctl(long boundMs, params string[] args)
		{
			List<string> command = new List<string> { CtlPath };
			command.AddRange(args);
			string text = "l5ctl " + string.Join(" ", args);
			ProcessStartInfo psi = new ProcessStartInfo
			{
				FileName = CtlPath,
				RedirectStandardInput = true,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				UseShellExecute = false,
				CreateNoWindow = true
			};

			foreach (string arg in args)
			{
				psi.ArgumentList.Add(arg);
			}

			Stopwatch sw = Stopwatch.StartNew();
			using Process process = Process.Start(psi);
			Assert.IsNotNull(process, text + " failed to start");

			StringBuilder stdout = new StringBuilder();
			StringBuilder stderr = new StringBuilder();
			process.OutputDataReceived += (_, e) => { if (e.Data != null) { stdout.AppendLine(e.Data); } };
			process.ErrorDataReceived += (_, e) => { if (e.Data != null) { stderr.AppendLine(e.Data); } };
			process.BeginOutputReadLine();
			process.BeginErrorReadLine();

			bool finished = process.WaitForExit((int)Math.Min(boundMs, int.MaxValue));
			long elapsedMs = sw.ElapsedMilliseconds;

			if (!finished)
			{
				try { process.Kill(entireProcessTree: true); } catch { /* best effort */ }
				Console.WriteLine("L5-CTL command=\"" + text + "\" exit=timeout elapsedMs=" + elapsedMs +
					" boundMs=" + boundMs);
				Assert.Fail(text + " did not finish within " + boundMs + "ms: " + stderr);
			}

			process.WaitForExit();
			string stdoutText = stdout.ToString();
			string stderrText = stderr.ToString().Trim();
			List<Line> lines = new List<Line>();

			foreach (string line in stdoutText.Split('\n'))
			{
				string trimmed = line.TrimEnd('\r');

				if (trimmed.StartsWith("L5-", StringComparison.Ordinal))
				{
					Console.WriteLine(trimmed);
					lines.Add(new Line(trimmed));
				}
			}

			Console.WriteLine("L5-CTL command=\"" + text + "\" exit=" + process.ExitCode +
				" elapsedMs=" + elapsedMs + " boundMs=" + boundMs);

			if (process.ExitCode != 0 && stderrText.Length > 0)
			{
				Console.WriteLine("L5-CTL-STDERR " + stderrText.Replace('\n', ' '));
			}

			return new Result(text, process.ExitCode, elapsedMs, lines, stderrText);
		}

		private static string Optional(string name)
		{
			return Environment.GetEnvironmentVariable(name) ?? "";
		}

		private static string Format(IReadOnlyList<Line> lines)
		{
			StringBuilder sb = new StringBuilder();

			foreach (Line line in lines)
			{
				if (sb.Length > 0)
				{
					sb.Append("; ");
				}

				sb.Append(line.Text);
			}

			return sb.ToString();
		}
	}
}
