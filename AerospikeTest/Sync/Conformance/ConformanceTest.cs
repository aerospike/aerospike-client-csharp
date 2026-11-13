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
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using Aerospike.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using YamlDotNet.Serialization;

namespace Aerospike.Test
{
	/// <summary>
	/// L1 conformance: run the shared aerospike-client-conformance fixture set and assert
	/// actual == declared for every row.
	/// </summary>
	[DoNotParallelize]
	[TestClass]
	public class ConformanceTest
	{
		private const string Pass = "pass";
		private const string Fail = "fail";
		private const string Unsupported = "unsupported";
		private const string Blocked = "blocked";
		private const string FixtureSetVersion = "1.1.0";
		private const string TlsRejectPrefix = "Invalid TLS name: ";
		private const string FixtureServer = "discovery/providers/fixture-server.py";
		private const int ProviderTimeout = 2000;
		private const string AccessKey = "AKIDCONFORMANCE00000";
		private const string SecretKey = "conformance-secret-key-not-real";
		private const string SessionToken = "conformance-session-token-not-real";

		private static readonly string[] FixtureRoots = { "discovery", "tls" };
		private static readonly HashSet<string> Results = new HashSet<string> { Pass, Fail, Unsupported, Blocked };
		private static readonly Dictionary<string, string> ErrorMarkers = new Dictionary<string, string>
		{
			["discovery-failed"] = "DNS seed discovery failed for every name",
			["aws-error"] = "DescribeInstances failed",
			["timeout"] = "timed out"
		};

		private static readonly ConcurrentBag<ReportRow> ReportRows = new ConcurrentBag<ReportRow>();
		private static readonly object InitLock = new object();
		private static bool initialized;
		private static string fixturesDir;
		private static string fixtureSetVersion;
		private static string reportPath;
		private static Process fixtureServer;
		private static int fixtureDnsPort;
		private static int fixtureHttpPort;
		private static HttpClient http;
		private static List<object[]> cases;

		public TestContext TestContext { get; set; }

		[ClassCleanup]
		public static void ClassCleanup()
		{
			StopFixtureServer();
			WriteReport();
		}

		[TestMethod]
		[DynamicData(nameof(GetCases))]
		public void Conformance(string name, Row row)
		{
			EnsureInitialized();

			// MSTest rejects an empty DynamicData source. When FIXTURES_DIRECTORY /
			// CONFORMANCE_DIR is unset this is not an L1 run: skip instead of failing the suite.
			if (row == null)
			{
				Assert.Inconclusive(
					"L1 conformance requires FIXTURES_DIRECTORY or CONFORMANCE_DIR " +
					"(aerospike-client-conformance checkout, fixture set " + FixtureSetVersion + ")");
				return;
			}

			Outcome actual = Execute(row);
			Declaration declared = row.Declared;
			string reason = (declared.Reason != null && declared.Result.Equals(actual.Result)) ?
				declared.Reason : actual.Reason;

			ReportRows.Add(new ReportRow
			{
				File = row.File,
				Case = row.Name,
				Result = actual.Result,
				Reason = reason,
				Detail = (actual.Reason != null && !string.Equals(actual.Reason, reason)) ? actual.Reason : null
			});

			Assert.AreEqual(declared.Result, actual.Result,
				row.File + ":" + row.Name + " declared " + declared.Result + ", actual " +
				actual.Result + (actual.Reason != null ? ": " + actual.Reason : ""));
		}

		public static IEnumerable<object[]> GetCases()
		{
			EnsureInitialized();

			if (cases.Count == 0)
			{
				yield return new object[] { "(not an L1 run)", null };
				yield break;
			}

			foreach (object[] row in cases)
			{
				yield return row;
			}
		}

		//-------------------------------------------------------
		// Init
		//-------------------------------------------------------

		private static void EnsureInitialized()
		{
			if (initialized)
			{
				return;
			}

			lock (InitLock)
			{
				if (initialized)
				{
					return;
				}

				string dirProperty = Environment.GetEnvironmentVariable("CONFORMANCE_DIR") ??
					Environment.GetEnvironmentVariable("FIXTURES_DIRECTORY");

				if (string.IsNullOrEmpty(dirProperty))
				{
					// Not an L1 run: discover zero cases so the normal suite is unaffected.
					cases = new List<object[]>();
					initialized = true;
					return;
				}

				fixturesDir = Path.GetFullPath(dirProperty);

				if (!Directory.Exists(fixturesDir))
				{
					throw new InvalidOperationException("Conformance directory not found: " + fixturesDir);
				}

				string versionPath = Path.Combine(fixturesDir, "VERSION");

				if (!File.Exists(versionPath))
				{
					throw new InvalidOperationException("Not a conformance fixture set, VERSION not found: " + fixturesDir);
				}

				fixtureSetVersion = File.ReadAllText(versionPath).Trim();

				if (!FixtureSetVersion.Equals(fixtureSetVersion))
				{
					throw new InvalidOperationException("Conformance fixture set " + fixtureSetVersion +
						" found, " + FixtureSetVersion + " expected: check out that release or update the declarations");
				}

				string reportEnv = Environment.GetEnvironmentVariable("CONFORMANCE_REPORT");
				reportPath = string.IsNullOrEmpty(reportEnv) ?
					Path.Combine("TestResults", "conformance-report.json") :
					Path.GetFullPath(reportEnv);

				List<string> files = new List<string>();

				foreach (string root in FixtureRoots)
				{
					string rootPath = Path.Combine(fixturesDir, root);

					if (!Directory.Exists(rootPath))
					{
						continue;
					}

					foreach (string path in Directory.EnumerateFiles(rootPath, "*.yaml", SearchOption.AllDirectories))
					{
						files.Add(path);
					}
				}

				files.Sort(StringComparer.Ordinal);

				Dictionary<string, Dictionary<string, Row>> rows = new Dictionary<string, Dictionary<string, Row>>();
				List<object[]> paramsList = new List<object[]>();

				foreach (string path in files)
				{
					string file = Path.GetRelativePath(fixturesDir, path).Replace('\\', '/');
					Dictionary<string, Row> fileRows = new Dictionary<string, Row>();

					foreach (Row row in LoadFile(path, file))
					{
						fileRows[row.Name] = row;
						paramsList.Add(new object[] { file + ":" + row.Name, row });
					}

					rows[file] = fileRows;
				}

				ApplyDeclarations(rows);

				foreach (object[] param in paramsList)
				{
					string kind = ((Row)param[1]).Kind;

					if (kind.Equals("dns") || kind.Equals("aws-tags"))
					{
						StartFixtureServer();
						break;
					}
				}

				cases = paramsList;
				initialized = true;
			}
		}

		//-------------------------------------------------------
		// Execution
		//-------------------------------------------------------

		private static Outcome Execute(Row row)
		{
			if (row.BlockedBy != null)
			{
				return new Outcome(Blocked, row.BlockedBy);
			}

			switch (row.Kind)
			{
				case "translate":
					return Translate(row);

				case "merge":
					return Merge(row);

				case "tls":
					return Tls(row);

				case "dns":
					return Provider(row, DnsProvider(row));

				case "aws-tags":
					return Provider(row, AwsProvider(row));

				default:
					throw new InvalidOperationException(row + ": unknown kind " + row.Kind);
			}
		}

		private static Outcome Translate(Row row)
		{
			Dictionary<string, object> config = AsMap(row.Data["config"]);
			Dictionary<string, object> input = AsMap(row.Data["input"]);
			Dictionary<string, object> expect = AsMap(row.Data["expect"]);
			Dictionary<string, object> translation = AsMap(Get(config, "translation"));
			ClientPolicy policy = new ClientPolicy();

			if (translation == null)
			{
				policy.ipMap = StringMap(Get(config, "ip_map"));
			}
			else
			{
				string provider = AsString(translation["provider"]);

				if (translation.ContainsKey("on_miss"))
				{
					policy.translationOnMiss = OnMiss(AsString(translation["on_miss"]));
				}

				switch (provider)
				{
					case "static-map":
						policy.addressTranslator = new StaticMapAddressTranslator(StringMap(Get(translation, "static_map")));
						break;

					case "pass-through":
						policy.addressTranslator = PassThroughAddressTranslator.Instance;
						break;

					case "port-map":
						policy.addressTranslator = new PortMapAddressTranslator(StringMap(Get(translation, "port_map")));
						break;

					default:
						return new Outcome(Unsupported, "translator " + provider + " is not implemented");
				}
			}

			Endpoint.SourceList sourceList = AsString(input["sourceList"]).Equals("alternate") ?
				Endpoint.SourceList.ALTERNATE : Endpoint.SourceList.STANDARD;
			Endpoint advertised = new Endpoint(
				AsString(Get(input, "nodeName")),
				AsString(Get(input, "tlsName")),
				AsString(input["host"]),
				AsInt(input["port"]),
				sourceList);
			Dictionary<string, object> error = AsMap(Get(expect, "error"));
			Host actual;

			try
			{
				actual = ConformanceAdapter.Translate(policy, advertised);
			}
			catch (AerospikeException.TranslationMiss tm)
			{
				if (error == null || !AsString(error["category"]).Equals("translation-miss"))
				{
					return new Outcome(Fail, "threw " + tm);
				}

			foreach (object value in AsMap(error["messageContains"]).Values)
			{
				string needle = Convert.ToString(value);

				if (!tm.Message.Contains(needle))
				{
					return new Outcome(Fail, "message does not contain " + needle + ": " + tm.Message);
				}
			}

				return new Outcome(Pass, null);
			}
			catch (Exception e)
			{
				return new Outcome(Fail, "threw " + e);
			}

			if (error != null)
			{
				return new Outcome(Fail, "expected " + error["category"] + " error, got " +
					EndpointString(actual.name, actual.port, actual.tlsName));
			}

			string expected = EndpointString(AsString(Get(expect, "host")), AsInt(expect["port"]), AsString(Get(expect, "tlsName")));
			string got = EndpointString(actual.name, actual.port, actual.tlsName);

			if (!expected.Equals(got))
			{
				return new Outcome(Fail, "expected " + expected + ", got " + got);
			}

			return new Outcome(Pass, null);
		}

		private static Outcome Merge(Row row)
		{
			Dictionary<string, object> config = AsMap(row.Data["config"]);
			Dictionary<string, object> discovery = AsMap(Get(config, "discovery"));
			ClientPolicy policy = new ClientPolicy();

			if (discovery != null && Get(discovery, "merge_policy") != null)
			{
				policy.seedMergePolicy = MergePolicy(AsString(discovery["merge_policy"]));
			}

			List<Host[]> results = new List<Host[]>();

			foreach (object result in AsList(AsMap(row.Data["input"])["discoveryResults"]))
			{
				results.Add(Hosts(result));
			}

			List<Host[]> snapshots = ConformanceAdapter.RefreshSeeds(policy, Hosts(Get(config, "seeds")), results);
			List<object> expected = AsList(AsMap(row.Data["expect"])["snapshots"]);

			for (int i = 0; i < expected.Count; i++)
			{
				string exp = Seeds(Hosts(expected[i]));
				string got = Seeds(snapshots[i]);

				if (!exp.Equals(got))
				{
					return new Outcome(Fail, "snapshot " + i + ": expected " + exp + ", got " + got);
				}
			}

			return new Outcome(Pass, null);
		}

		private static Outcome Tls(Row row)
		{
			string tlsName = AsString(AsMap(row.Data["input"])["tlsName"]);
			string expected = AsString(AsMap(row.Data["expect"])["outcome"]);
			string got;

			try
			{
				TlsConnection.ValidateServerCertificate(tlsName, row.Certificate);
				got = "accept";
			}
			catch (AerospikeException ae)
			{
				if (!ae.BaseMessage.StartsWith(TlsRejectPrefix, StringComparison.Ordinal))
				{
					return new Outcome(Fail, "threw " + ae);
				}

				got = "reject";
			}

			if (!expected.Equals(got))
			{
				return new Outcome(Fail, "expected " + expected + ", got " + got);
			}

			return new Outcome(Pass, null);
		}

		private static DnsSeedCandidateProvider DnsProvider(Row row)
		{
			Dictionary<string, object> dns = AsMap(AsMap(AsMap(row.Data["config"])["discovery"])["dns"]);
			List<string> names = new List<string>();

			foreach (object name in AsList(dns["names"]))
			{
				names.Add(AsString(name));
			}

			DnsSeedCandidateProvider.Builder builder = DnsSeedCandidateProvider.CreateBuilder()
				.Names(names.ToArray())
				.Timeout(IntOr(Get(dns, "timeout_ms"), ProviderTimeout))
				.Servers("127.0.0.1:" + fixtureDnsPort);

			if (Get(dns, "record_type") != null)
			{
				builder.RecordType(AsString(dns["record_type"]).Equals("srv", StringComparison.OrdinalIgnoreCase) ?
					DnsSeedCandidateProvider.RecordType.SRV :
					DnsSeedCandidateProvider.RecordType.A);
			}

			if (Get(dns, "port") != null)
			{
				builder.Port(AsInt(dns["port"]));
			}

			if (Get(dns, "tls_name") != null)
			{
				builder.TlsName(AsString(dns["tls_name"]));
			}

			return builder.Build();
		}

		private static AwsEc2TagSeedCandidateProvider AwsProvider(Row row)
		{
			Dictionary<string, object> aws = AsMap(AsMap(AsMap(row.Data["config"])["discovery"])["aws"]);
			int timeout = IntOr(Get(aws, "timeout_ms"), ProviderTimeout);
			AwsEc2TagSeedCandidateProvider.Builder builder = AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region(AsString(aws["region"]))
				.Timeout(timeout)
				.InitTimeout(IntOr(Get(aws, "init_timeout_ms"), Math.Max(timeout, ProviderTimeout)))
				.Endpoint("http://127.0.0.1:" + fixtureHttpPort)
				.Credentials(AccessKey, SecretKey, SessionToken);

			foreach (object item in AsList(aws["filters"]))
			{
				Dictionary<string, object> filter = AsMap(item);
				List<string> values = new List<string>();

				foreach (object value in AsList(filter["values"]))
				{
					values.Add(AsString(value));
				}

				builder.Filter(AsString(filter["name"]), values.ToArray());
			}

			if (Get(aws, "address_field") != null)
			{
				builder.AddressField(ParseAddressField(AsString(aws["address_field"])));
			}

			if (Get(aws, "port") != null)
			{
				builder.Port(AsInt(aws["port"]));
			}

			if (Get(aws, "tls_name") != null)
			{
				builder.TlsName(AsString(aws["tls_name"]));
			}

			return builder.Build();
		}

		private static AwsEc2TagSeedCandidateProvider.AddressField ParseAddressField(string value)
		{
			return value.ToLowerInvariant() switch
			{
				"private_ip" => AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_IP,
				"public_ip" => AwsEc2TagSeedCandidateProvider.AddressField.PUBLIC_IP,
				"private_dns" => AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_DNS,
				"public_dns" => AwsEc2TagSeedCandidateProvider.AddressField.PUBLIC_DNS,
				_ => throw new InvalidOperationException("unknown address_field " + value)
			};
		}

		private static Outcome Provider(Row row, ISeedCandidateProvider provider)
		{
			Dictionary<string, object> config = AsMap(AsMap(row.Data["config"])["discovery"]);
			Dictionary<string, object> aws = AsMap(Get(config, "aws"));
			string region = (aws != null) ? AsString(Get(aws, "region")) : null;
			List<object> refreshes = AsList(AsMap(row.Data["input"])["refreshes"]);
			List<object> expected = AsList(AsMap(row.Data["expect"])["refreshes"]);

			List<ConformanceAdapter.Step> steps = ConformanceAdapter.RefreshProvider(provider, refreshes.Count,
				i => SelectRefresh(refreshes[i], region));

			for (int i = 0; i < expected.Count; i++)
			{
				Dictionary<string, object> exp = AsMap(expected[i]);
				ConformanceAdapter.Step step = steps[i];
				Dictionary<string, object> error = AsMap(Get(exp, "error"));

				if (error != null)
				{
					if (step.error == null)
					{
						return new Outcome(Fail, "refresh " + i + ": expected " + error["category"] + ", got " +
							HostSet(step.hosts));
					}

					List<string> facts = new List<string> { ErrorMarkers[AsString(error["category"])] };

					foreach (object fact in AsList(Get(error, "messageContains")))
					{
						facts.Add(AsString(fact));
					}

					foreach (string fact in facts)
					{
						if (fact != null && !step.error.Contains(fact))
						{
							return new Outcome(Fail, "refresh " + i + ": message lacks '" + fact + "': " + step.error);
						}
					}

					foreach (string secret in new[] { SecretKey, SessionToken })
					{
						if (step.error.Contains(secret))
						{
							return new Outcome(Fail, "refresh " + i + ": message contains a credential secret");
						}
					}
				}
				else
				{
					if (step.error != null)
					{
						return new Outcome(Fail, "refresh " + i + ": failed: " + step.error);
					}

					SortedSet<string> expHosts = HostSet(AsList(exp["hosts"]));
					SortedSet<string> got = HostSet(step.hosts);

					if (!expHosts.SetEquals(got))
					{
						return new Outcome(Fail, "refresh " + i + ": expected hosts " + FormatSet(expHosts) + ", got " + FormatSet(got));
					}
				}

				SortedSet<string> expSeeds = HostSet(AsList(exp["seeds"]));
				SortedSet<string> gotSeeds = HostSet(step.seeds);

				if (!expSeeds.SetEquals(gotSeeds))
				{
					return new Outcome(Fail, "refresh " + i + ": expected seeds " + FormatSet(expSeeds) + ", got " + FormatSet(gotSeeds));
				}
			}

			return new Outcome(Pass, null);
		}

		private static void SelectRefresh(object refresh, string region)
		{
			Dictionary<string, object> selection = new Dictionary<string, object>
			{
				["refresh"] = refresh,
				["region"] = region
			};
			StringBuilder sb = new StringBuilder();
			WriteJson(sb, selection, "");

			try
			{
				using HttpResponseMessage response = http.PostAsync(
					"http://127.0.0.1:" + fixtureHttpPort + "/fixture/select",
					new StringContent(sb.ToString(), Encoding.UTF8, "application/json")).GetAwaiter().GetResult();
				string text = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

				if ((int)response.StatusCode != 200)
				{
					throw new InvalidOperationException("fixture server rejected the refresh: " + text);
				}
			}
			catch (InvalidOperationException)
			{
				throw;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("fixture server unreachable", e);
			}
		}

		//-------------------------------------------------------
		// Helpers
		//-------------------------------------------------------

		private static SortedSet<string> HostSet(IEnumerable<object> hosts)
		{
			SortedSet<string> set = new SortedSet<string>(StringComparer.Ordinal);

			if (hosts == null)
			{
				return set;
			}

			foreach (object item in hosts)
			{
				string host;
				int port;
				string tlsName;

				if (item is Host h)
				{
					host = h.name;
					port = h.port;
					tlsName = h.tlsName;
				}
				else
				{
					Dictionary<string, object> map = AsMap(item);
					host = AsString(map["host"]);
					port = AsInt(map["port"]);
					tlsName = AsString(Get(map, "tlsName"));
				}

				set.Add(CanonicalHost(host) + " " + port + ((tlsName != null) ? " " + tlsName : ""));
			}

			return set;
		}

		private static SortedSet<string> HostSet(IList<Host> hosts)
		{
			return HostSet(hosts?.Cast<object>());
		}

		private static SortedSet<string> HostSet(Host[] hosts)
		{
			return HostSet((IEnumerable<object>)hosts);
		}

		private static string CanonicalHost(string host)
		{
			if (Regex.IsMatch(host, @"^[0-9.]+$") || host.Contains(':'))
			{
				try
				{
					return IPAddress.Parse(host).ToString();
				}
				catch
				{
					return host;
				}
			}

			return host;
		}

		private static string FormatSet(SortedSet<string> set)
		{
			return "[" + string.Join(", ", set) + "]";
		}

		private static int IntOr(object value, int otherwise)
		{
			return (value != null) ? AsInt(value) : otherwise;
		}

		private static TranslationMissMode OnMiss(string value)
		{
			return value switch
			{
				"pass_through" => TranslationMissMode.PASS_THROUGH,
				"strict" => TranslationMissMode.STRICT,
				_ => throw new InvalidOperationException("unknown on_miss " + value)
			};
		}

		private static SeedMergePolicy MergePolicy(string value)
		{
			return value switch
			{
				"merge" => SeedMergePolicy.MERGE,
				"replace" => SeedMergePolicy.REPLACE,
				"discovery_only" => SeedMergePolicy.DISCOVERY_ONLY,
				_ => throw new InvalidOperationException("unknown merge_policy " + value)
			};
		}

		private static Host[] Hosts(object value)
		{
			if (value == null)
			{
				return Array.Empty<Host>();
			}

			List<object> list = AsList(value);
			Host[] hosts = new Host[list.Count];

			for (int i = 0; i < hosts.Length; i++)
			{
				Dictionary<string, object> seed = AsMap(list[i]);
				hosts[i] = new Host(AsString(seed["host"]), AsInt(seed["port"]));
			}

			return hosts;
		}

		private static string Seeds(Host[] hosts)
		{
			return "[" + string.Join(", ", Array.ConvertAll(hosts, h => h.name + " " + h.port)) + "]";
		}

		private static string EndpointString(string host, int port, string tlsName)
		{
			return "host=" + host + " port=" + port + " tlsName=" + tlsName;
		}

		//-------------------------------------------------------
		// Fixtures / YAML
		//-------------------------------------------------------

		private static List<Row> LoadFile(string path, string file)
		{
			IDeserializer deserializer = new DeserializerBuilder().Build();
			Dictionary<string, object> top;

			using (StreamReader reader = File.OpenText(path))
			{
				top = AsMap(deserializer.Deserialize(reader));
			}

			string kind = AsString(top["kind"]);
			Dictionary<string, X509Certificate2> certificates = new Dictionary<string, X509Certificate2>();
			List<Row> rows = new List<Row>();

			foreach (object item in AsList(top["rows"]))
			{
				Dictionary<string, object> data = AsMap(item);
				X509Certificate2 certificate = null;

				if (kind.Equals("tls"))
				{
					string shape = AsString(AsMap(data["input"])["shape"]);
					string certPath = Path.Combine(Path.GetDirectoryName(path), "certs", shape + ".pem");

					if (!certificates.TryGetValue(shape, out certificate))
					{
						certificate = X509Certificate2.CreateFromPem(File.ReadAllText(certPath));
						certificates[shape] = certificate;
					}
				}

				string blockedBy = Blocked.Equals(AsString(Get(data, "state"))) ? AsString(Get(data, "blockedBy")) : null;
				rows.Add(new Row(file, kind, AsString(data["case"]), blockedBy, data, certificate));
			}

			return rows;
		}

		private static void ApplyDeclarations(Dictionary<string, Dictionary<string, Row>> rows)
		{
			string declaredPath = Path.Combine(FindDeclarationsDir(), "csharp-declared.yaml");

			if (!File.Exists(declaredPath))
			{
				throw new InvalidOperationException("Declarations not found: " + declaredPath);
			}

			IDeserializer deserializer = new DeserializerBuilder().Build();
			object doc;

			using (StreamReader reader = File.OpenText(declaredPath))
			{
				doc = deserializer.Deserialize(reader);
			}

			Dictionary<string, object> files = (doc == null) ?
				new Dictionary<string, object>() :
				RequireDeclaration(doc, declaredPath);

			foreach (KeyValuePair<string, object> file in files)
			{
				if (!rows.TryGetValue(file.Key, out Dictionary<string, Row> fileRows))
				{
					throw new InvalidOperationException(declaredPath + ": unknown fixture file " + file.Key);
				}

				foreach (KeyValuePair<string, object> c in RequireDeclaration(file.Value, file.Key))
				{
					string where = declaredPath + ": " + file.Key + ":" + c.Key;

					if (!fileRows.TryGetValue(c.Key, out Row row))
					{
						throw new InvalidOperationException(where + ": unknown case");
					}

					Dictionary<string, object> decl = RequireDeclaration(c.Value, where);

					foreach (string key in decl.Keys)
					{
						if (!key.Equals("result") && !key.Equals("reason"))
						{
							throw new InvalidOperationException(where + ": unknown key " + key);
						}
					}

					string result = AsString(Get(decl, "result"));
					string reason = AsString(Get(decl, "reason"));

					if (!Results.Contains(result))
					{
						throw new InvalidOperationException(where + ": invalid result " + result);
					}

					if (!Pass.Equals(result) && string.IsNullOrEmpty(reason))
					{
						throw new InvalidOperationException(where + ": a " + result + " declaration needs a reason");
					}

					row.Declared = new Declaration(result, reason);
				}
			}
		}

		private static string FindDeclarationsDir()
		{
			string env = Environment.GetEnvironmentVariable("CONFORMANCE_DECLARATIONS");

			if (!string.IsNullOrEmpty(env))
			{
				return Path.GetFullPath(env);
			}

			string baseDir = AppContext.BaseDirectory;
			string[] candidates =
			{
				Path.Combine(baseDir, "conformance"),
				Path.Combine(baseDir, "..", "..", "..", "conformance"),
				Path.Combine(Directory.GetCurrentDirectory(), "AerospikeTest", "conformance"),
				Path.Combine(Directory.GetCurrentDirectory(), "conformance")
			};

			foreach (string candidate in candidates)
			{
				string full = Path.GetFullPath(candidate);

				if (File.Exists(Path.Combine(full, "csharp-declared.yaml")))
				{
					return full;
				}
			}

			throw new InvalidOperationException("Could not find AerospikeTest/conformance/csharp-declared.yaml");
		}

		private static Dictionary<string, object> RequireDeclaration(object value, string where)
		{
			Dictionary<string, object> map = AsMap(value);

			if (map == null)
			{
				throw new InvalidOperationException("csharp-declared.yaml: " + where + " must be a map");
			}

			return map;
		}

		private static Dictionary<string, object> AsMap(object value)
		{
			if (value == null)
			{
				return null;
			}

			if (value is Dictionary<string, object> typed)
			{
				return typed;
			}

			if (value is IDictionary<object, object> objMap)
			{
				Dictionary<string, object> map = new Dictionary<string, object>();

				foreach (KeyValuePair<object, object> entry in objMap)
				{
					map[Convert.ToString(entry.Key)] = entry.Value;
				}

				return map;
			}

			if (value is System.Collections.IDictionary dict)
			{
				Dictionary<string, object> map = new Dictionary<string, object>();

				foreach (System.Collections.DictionaryEntry entry in dict)
				{
					map[Convert.ToString(entry.Key)] = entry.Value;
				}

				return map;
			}

			throw new InvalidOperationException("expected a map, got " + value.GetType());
		}

		private static List<object> AsList(object value)
		{
			if (value == null)
			{
				return new List<object>();
			}

			if (value is List<object> typed)
			{
				return typed;
			}

			if (value is System.Collections.IEnumerable enumerable && value is not string)
			{
				List<object> list = new List<object>();

				foreach (object item in enumerable)
				{
					list.Add(item);
				}

				return list;
			}

			throw new InvalidOperationException("expected a list, got " + value.GetType());
		}

		private static Dictionary<string, string> StringMap(object value)
		{
			Dictionary<string, object> map = AsMap(value);

			if (map == null)
			{
				return null;
			}

			Dictionary<string, string> result = new Dictionary<string, string>();

			foreach (KeyValuePair<string, object> entry in map)
			{
				result[entry.Key] = entry.Value == null ? null : Convert.ToString(entry.Value);
			}

			return result;
		}

		private static object Get(Dictionary<string, object> map, string key)
		{
			if (map == null)
			{
				return null;
			}

			return map.TryGetValue(key, out object value) ? value : null;
		}

		private static string AsString(object value)
		{
			return value == null ? null : Convert.ToString(value);
		}

		private static int AsInt(object value)
		{
			return Convert.ToInt32(value);
		}

		//-------------------------------------------------------
		// Fixture server / report
		//-------------------------------------------------------

		private static void StartFixtureServer()
		{
			string script = Path.Combine(fixturesDir, FixtureServer.Replace('/', Path.DirectorySeparatorChar));

			if (!File.Exists(script))
			{
				throw new InvalidOperationException("Fixture server not found: " + script);
			}

			string python = FindPython();
			string logDir = Path.GetDirectoryName(reportPath);

			if (!string.IsNullOrEmpty(logDir))
			{
				Directory.CreateDirectory(logDir);
			}

			string log = Path.Combine(logDir ?? ".", "conformance-fixture-server.log");
			ProcessStartInfo psi = new ProcessStartInfo
			{
				FileName = python,
				Arguments = "\"" + script + "\"",
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				UseShellExecute = false,
				CreateNoWindow = true
			};
			fixtureServer = Process.Start(psi);
			string line = fixtureServer.StandardOutput.ReadLine();

			if (line == null)
			{
				string err = fixtureServer.StandardError.ReadToEnd();
				File.WriteAllText(log, err);
				throw new InvalidOperationException("Fixture server exited during startup, see " + log);
			}

			Match httpMatch = Regex.Match(line, "\"http\": (\\d+)");
			Match dnsMatch = Regex.Match(line, "\"dns\": (\\d+)");

			if (!httpMatch.Success || !dnsMatch.Success)
			{
				throw new InvalidOperationException("Unexpected fixture server ports line: " + line);
			}

			fixtureHttpPort = int.Parse(httpMatch.Groups[1].Value);
			fixtureDnsPort = int.Parse(dnsMatch.Groups[1].Value);
			http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
			Task.Run(() =>
			{
				string err = fixtureServer.StandardError.ReadToEnd();
				File.AppendAllText(log, err);
			});
		}

		private static string FindPython()
		{
			foreach (string candidate in new[] { "python3", "python" })
			{
				try
				{
					ProcessStartInfo psi = new ProcessStartInfo
					{
						FileName = candidate,
						Arguments = "--version",
						RedirectStandardOutput = true,
						RedirectStandardError = true,
						UseShellExecute = false,
						CreateNoWindow = true
					};
					using Process check = Process.Start(psi);
					check.WaitForExit(10000);

					if (check.ExitCode == 0)
					{
						return candidate;
					}
				}
				catch
				{
					// try next
				}
			}

			throw new InvalidOperationException("python3 is required to run the dns and aws-tags fixtures");
		}

		private static void StopFixtureServer()
		{
			if (fixtureServer == null)
			{
				return;
			}

			try
			{
				if (!fixtureServer.HasExited)
				{
					fixtureServer.Kill(entireProcessTree: true);
					fixtureServer.WaitForExit(5000);
				}
			}
			catch
			{
				// ignore
			}
			finally
			{
				fixtureServer.Dispose();
				fixtureServer = null;
				http?.Dispose();
				http = null;
			}
		}

		private static void WriteReport()
		{
			if (!initialized || string.IsNullOrEmpty(reportPath))
			{
				return;
			}

			Dictionary<string, int> counts = new Dictionary<string, int>
			{
				[Pass] = 0,
				[Fail] = 0,
				[Unsupported] = 0,
				[Blocked] = 0
			};

			List<ReportRow> sorted = ReportRows.OrderBy(r => r.File + ":" + r.Case, StringComparer.Ordinal).ToList();

			foreach (ReportRow entry in sorted)
			{
				counts[entry.Result] = counts[entry.Result] + 1;
			}

			var report = new Dictionary<string, object>
			{
				["runtime"] = "csharp",
				["clientVersion"] = ClientVersion(),
				["fixtureSetVersion"] = fixtureSetVersion,
				["counts"] = counts,
				["rows"] = sorted.Select(r =>
				{
					var map = new Dictionary<string, object>
					{
						["file"] = r.File,
						["case"] = r.Case,
						["result"] = r.Result
					};

					if (r.Reason != null)
					{
						map["reason"] = r.Reason;
					}

					if (r.Detail != null)
					{
						map["detail"] = r.Detail;
					}

					return map;
				}).ToList()
			};

			string fullReport = Path.GetFullPath(reportPath);
			string parent = Path.GetDirectoryName(fullReport);

			if (!string.IsNullOrEmpty(parent))
			{
				Directory.CreateDirectory(parent);
			}

			StringBuilder sb = new StringBuilder();
			WriteJson(sb, report, "");
			sb.Append('\n');
			File.WriteAllText(fullReport, sb.ToString());
		}

		private static string ClientVersion()
		{
			Version version = typeof(AerospikeClient).Assembly.GetName().Version;
			return version != null ? version.ToString(3) : "unknown";
		}

		private static void WriteJson(StringBuilder sb, object value, string indent)
		{
			value = CoerceYamlScalar(value);

			if (value is System.Collections.IDictionary map)
			{
				string inner = indent + "  ";
				sb.Append('{');
				int i = 0;

				foreach (System.Collections.DictionaryEntry entry in map)
				{
					sb.Append(i++ == 0 ? "\n" : ",\n").Append(inner);
					WriteJsonString(sb, Convert.ToString(entry.Key));
					sb.Append(": ");
					WriteJson(sb, entry.Value, inner);
				}

				sb.Append('\n').Append(indent).Append('}');
			}
			else if (value is System.Collections.IList list && value is not string)
			{
				bool scalars = true;

				foreach (object item in list)
				{
					object coerced = CoerceYamlScalar(item);

					if (coerced is System.Collections.IDictionary ||
						(coerced is System.Collections.IList && coerced is not string))
					{
						scalars = false;
						break;
					}
				}

				string inner = indent + "  ";
				sb.Append('[');

				for (int i = 0; i < list.Count; i++)
				{
					if (scalars)
					{
						sb.Append(i == 0 ? "" : ", ");
					}
					else
					{
						sb.Append(i == 0 ? "\n" : ",\n").Append(inner);
					}

					WriteJson(sb, list[i], inner);
				}

				if (!scalars && list.Count > 0)
				{
					sb.Append('\n').Append(indent);
				}

				sb.Append(']');
			}
			else if (value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal)
			{
				sb.Append(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
			}
			else if (value is bool b)
			{
				sb.Append(b ? "true" : "false");
			}
			else if (value == null)
			{
				sb.Append("null");
			}
			else
			{
				WriteJsonString(sb, Convert.ToString(value));
			}
		}

		/// <summary>
		/// YamlDotNet's untyped deserialize uses the failsafe schema, so every scalar is a string.
		/// Coerce JSON-compatible bools and integers so the fixture server receives real numbers.
		/// </summary>
		private static object CoerceYamlScalar(object value)
		{
			if (value is not string s)
			{
				return value;
			}

			if (s.Equals("true", StringComparison.Ordinal))
			{
				return true;
			}

			if (s.Equals("false", StringComparison.Ordinal))
			{
				return false;
			}

			if (s.Equals("null", StringComparison.Ordinal) || s.Equals("~", StringComparison.Ordinal))
			{
				return null;
			}

			if (long.TryParse(s, System.Globalization.NumberStyles.Integer,
				System.Globalization.CultureInfo.InvariantCulture, out long n) &&
				n.ToString(System.Globalization.CultureInfo.InvariantCulture).Equals(s))
			{
				return n;
			}

			return value;
		}

		private static void WriteJsonString(StringBuilder sb, string s)
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
						sb.Append(c);
						break;
				}
			}

			sb.Append('"');
		}

		//-------------------------------------------------------
		// Types
		//-------------------------------------------------------

		public sealed class Row
		{
			internal readonly string File;
			internal readonly string Kind;
			internal readonly string Name;
			internal readonly string BlockedBy;
			internal readonly Dictionary<string, object> Data;
			internal readonly X509Certificate2 Certificate;
			internal Declaration Declared;

			internal Row(string file, string kind, string name, string blockedBy,
				Dictionary<string, object> data, X509Certificate2 certificate)
			{
				File = file;
				Kind = kind;
				Name = name;
				BlockedBy = blockedBy;
				Data = data;
				Certificate = certificate;
				Declared = new Declaration((blockedBy != null) ? Blocked : Pass, null);
			}

			public override string ToString()
			{
				return File + ":" + Name;
			}
		}

		internal sealed class Declaration
		{
			internal readonly string Result;
			internal readonly string Reason;

			internal Declaration(string result, string reason)
			{
				Result = result;
				Reason = reason;
			}
		}

		private sealed class Outcome
		{
			internal readonly string Result;
			internal readonly string Reason;

			internal Outcome(string result, string reason)
			{
				Result = result;
				Reason = reason;
			}
		}

		private sealed class ReportRow
		{
			public string File { get; set; }
			public string Case { get; set; }
			public string Result { get; set; }
			public string Reason { get; set; }
			public string Detail { get; set; }
		}
	}
}
