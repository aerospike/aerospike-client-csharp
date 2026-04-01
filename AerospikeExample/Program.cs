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
using Aerospike.Client;
using System.Xml.Linq;

namespace Aerospike.Example;

public class Program
{
	private static readonly string[] ExampleNames =
	{
		"ServerInfo",
		"PutGet",
		"Exists",
		"Delete",
		"Replace",
		"Add",
		"Append",
		"Prepend",
		"Batch",
		"BatchOperate",
		"Generation",
		"Expire",
		"Touch",
		"Transaction",
		"Operate",
		"OperateBit",
		"OperateList",
		"OperateMap",
		"DeleteBin",
		"GetAndJoin",
		"ScanParallel",
		"ScanSeries",
		"ScanPage",
		"ScanResume",
		"ListMap",
		"UserDefinedFunction",
		"QueryInteger",
		"QueryString",
		"QueryList",
		"QueryRegion",
		"QueryRegionFilter",
		"QueryFilter",
		"QueryExp",
		"QueryPage",
		"QueryResume",
		"QuerySum",
		"QueryAverage",
		"QueryExecute",
		"QueryGeoCollection",
		"QueryOpsProjection",
		"PathExpression",
		"PathExpressionEnhanced",
		"AsyncPutGet",
		"AsyncBatch",
		"AsyncScan",
		"AsyncScanPage",
		"AsyncTransaction",
		"AsyncTransactionWithTask",
		"AsyncQuery",
		"AsyncUserDefinedFunction"
	};

	static int Main(string[] args)
	{
		try
		{
			var arguments = ParseArguments(args);

			if (arguments == null)
			{
				return 1;
			}

			if (arguments.syncExamples.Count == 0 && arguments.asyncExamples.Count == 0)
			{
				PrintUsage();
				return 1;
			}

			var console = new Console();
			var results = RunExamples(console, arguments);
			return PrintSummary(results);
		}
		catch (Exception ex)
		{
			System.Console.Error.WriteLine($"Error: {ex.Message}");
			System.Console.Error.WriteLine(ex.StackTrace);
			return 1;
		}
	}

	private static Arguments ParseArguments(string[] args)
	{
		if (args.Length == 0)
		{
			PrintUsage();
			return null;
		}

		// Defaults
		string host = "127.0.0.1";
		int port = 3000;
		string ns = "test";
		string set = "demoset";
		string user = null;
		string password = null;
		string clusterName = null;
		bool tlsEnable = false;
		string tlsName = null;
		string tlsProtocols = null;
		string tlsRevoke = null;
		string tlsClientCertFile = null;
		bool tlsLoginOnly = false;
		AuthMode authMode = AuthMode.INTERNAL;
		bool useServicesAlternate = false;
		bool debug = false;
		int commandMax = 40;
		string settingsPath = null;
		List<string> exampleNames = [];

		// Track which values were explicitly set on the command line
		HashSet<string> cliOverrides = [];

		int i = 0;

		while (i < args.Length)
		{
			string arg = args[i];

			switch (arg)
			{
				case "-h":
				case "--host":
					if (++i >= args.Length) { PrintUsage(); return null; }
					host = args[i];
					cliOverrides.Add("Host");
					break;

				case "-p":
				case "--port":
					if (++i >= args.Length) { PrintUsage(); return null; }
					port = int.Parse(args[i]);
					cliOverrides.Add("Port");
					break;

				case "-U":
				case "--user":
					if (++i >= args.Length) { PrintUsage(); return null; }
					user = args[i];
					cliOverrides.Add("User");
					break;

				case "-P":
				case "--password":
					if (++i >= args.Length) { PrintUsage(); return null; }
					password = args[i];
					cliOverrides.Add("Password");
					break;

				case "-n":
				case "--namespace":
					if (++i >= args.Length) { PrintUsage(); return null; }
					ns = args[i];
					cliOverrides.Add("Namespace");
					break;

				case "-s":
				case "--set":
					if (++i >= args.Length) { PrintUsage(); return null; }
					set = args[i];
					if (set.Equals("empty", StringComparison.OrdinalIgnoreCase))
					{
						set = "";
					}
					cliOverrides.Add("Set");
					break;

				case "-c":
				case "--clusterName":
					if (++i >= args.Length) { PrintUsage(); return null; }
					clusterName = args[i];
					cliOverrides.Add("ClusterName");
					break;

				case "--tls":
				case "--tlsEnable":
					tlsEnable = true;
					cliOverrides.Add("TlsEnable");
					break;

				case "--tlsName":
					if (++i >= args.Length) { PrintUsage(); return null; }
					tlsName = args[i];
					cliOverrides.Add("TlsName");
					break;

				case "--tlsProtocols":
					if (++i >= args.Length) { PrintUsage(); return null; }
					tlsProtocols = args[i];
					cliOverrides.Add("TlsProtocols");
					break;

				case "--tlsRevoke":
					if (++i >= args.Length) { PrintUsage(); return null; }
					tlsRevoke = args[i];
					cliOverrides.Add("TlsRevoke");
					break;

				case "--tlsClientCertFile":
					if (++i >= args.Length) { PrintUsage(); return null; }
					tlsClientCertFile = args[i];
					cliOverrides.Add("TlsClientCertFile");
					break;

				case "--tlsLoginOnly":
					tlsLoginOnly = true;
					cliOverrides.Add("TlsLoginOnly");
					break;

				case "--auth":
					if (++i >= args.Length) { PrintUsage(); return null; }
					authMode = (AuthMode)Enum.Parse(typeof(AuthMode), args[i], true);
					cliOverrides.Add("AuthMode");
					break;

				case "--useServicesAlternate":
					useServicesAlternate = true;
					cliOverrides.Add("UseServicesAlternate");
					break;

				case "--commandMax":
					if (++i >= args.Length) { PrintUsage(); return null; }
					commandMax = int.Parse(args[i]);
					break;

				case "--settings":
					if (++i >= args.Length) { PrintUsage(); return null; }
					settingsPath = args[i];
					break;

				case "-d":
				case "--debug":
					debug = true;
					break;

				case "-u":
				case "--usage":
				case "--help":
					PrintUsage();
					return null;

				default:
					exampleNames.Add(arg);
					break;
			}
			i++;
		}

		// Load .runsettings if specified -- values are used as defaults
		// that CLI args override.
		if (settingsPath != null)
		{
			var settings = LoadRunSettings(settingsPath);

			if (!cliOverrides.Contains("Host") && settings.TryGetValue("Host", out string h) && !string.IsNullOrEmpty(h))
				host = h;
			if (!cliOverrides.Contains("Port") && settings.TryGetValue("Port", out string p))
				port = int.Parse(p);
			if (!cliOverrides.Contains("Namespace") && settings.TryGetValue("Namespace", out string n))
				ns = n;
			if (!cliOverrides.Contains("Set") && settings.TryGetValue("Set", out string s))
				set = s;
			if (!cliOverrides.Contains("User") && settings.TryGetValue("User", out string u) && !string.IsNullOrEmpty(u))
				user = u;
			if (!cliOverrides.Contains("Password") && settings.TryGetValue("Password", out string pw) && !string.IsNullOrEmpty(pw))
				password = pw;
			if (!cliOverrides.Contains("ClusterName") && settings.TryGetValue("ClusterName", out string cn) && !string.IsNullOrEmpty(cn))
				clusterName = cn;
			if (!cliOverrides.Contains("AuthMode") && settings.TryGetValue("AuthMode", out string am) && !string.IsNullOrEmpty(am))
				authMode = (AuthMode)Enum.Parse(typeof(AuthMode), am, true);
			if (!cliOverrides.Contains("UseServicesAlternate") && settings.TryGetValue("UseServicesAlternate", out string usa))
				useServicesAlternate = bool.Parse(usa);

			if (!cliOverrides.Contains("TlsEnable") && settings.TryGetValue("TlsEnable", out string te))
				tlsEnable = bool.Parse(te);
			if (!cliOverrides.Contains("TlsName") && settings.TryGetValue("TlsName", out string tn) && !string.IsNullOrEmpty(tn))
				tlsName = tn;
			if (!cliOverrides.Contains("TlsProtocols") && settings.TryGetValue("TlsProtocols", out string tp) && !string.IsNullOrEmpty(tp))
				tlsProtocols = tp;
			if (!cliOverrides.Contains("TlsRevoke") && settings.TryGetValue("TlsRevoke", out string tr) && !string.IsNullOrEmpty(tr))
				tlsRevoke = tr;
			if (!cliOverrides.Contains("TlsClientCertFile") && settings.TryGetValue("TlsClientCertFile", out string tcf) && !string.IsNullOrEmpty(tcf))
				tlsClientCertFile = tcf;
			if (!cliOverrides.Contains("TlsLoginOnly") && settings.TryGetValue("TlsLoginOnly", out string tlo))
				tlsLoginOnly = bool.Parse(tlo);
		}

		// Expand "all"
		for (int j = 0; j < exampleNames.Count; j++)
		{
			if (exampleNames[j].Equals("all", StringComparison.OrdinalIgnoreCase))
			{
				exampleNames = new List<string>(ExampleNames);
				break;
			}
		}

		TlsPolicy tlsPolicy = null;

		if (tlsEnable)
		{
			tlsPolicy = new TlsPolicy(
				tlsProtocols,
				tlsRevoke,
				tlsClientCertFile,
				tlsLoginOnly
			);
		}

		Log.SetLevel(debug ? Log.Level.DEBUG : Log.Level.INFO);

		var arguments = new Arguments()
		{
			hosts = Host.ParseHosts(host, tlsName, port),
			port = port,
			ns = ns,
			set = set,
			user = user,
			password = password,
			clusterName = clusterName,
			tlsPolicy = tlsPolicy,
			authMode = authMode,
			useServicesAlternate = useServicesAlternate,
			commandMax = commandMax
		};

		// Split examples into sync and async lists
		foreach (string name in exampleNames)
		{
			if (name.StartsWith("Async", StringComparison.OrdinalIgnoreCase))
			{
				arguments.asyncExamples.Add(name);
			}
			else
			{
				arguments.syncExamples.Add(name);
			}
		}

		return arguments;
	}

	/// <summary>
	/// Parse a .runsettings XML file and extract TestRunParameters as a dictionary.
	/// </summary>
	private static Dictionary<string, string> LoadRunSettings(string path)
	{
		var settings = new Dictionary<string, string>();

		if (!File.Exists(path))
		{
			throw new FileNotFoundException($"Settings file not found: {path}");
		}

		var doc = XDocument.Load(path);
		var testRunParams = doc.Descendants("TestRunParameters").FirstOrDefault();

		if (testRunParams == null)
		{
			return settings;
		}

		foreach (var param in testRunParams.Elements("Parameter"))
		{
			string name = param.Attribute("name")?.Value;
			string value = param.Attribute("value")?.Value;

			if (name != null)
			{
				settings[name] = value ?? "";
			}
		}

		return settings;
	}

	private static List<ExampleResultInfo> RunExamples(Console console, Arguments args)
	{
		var results = new List<ExampleResultInfo>();

		if (args.syncExamples.Count > 0)
		{
			results.AddRange(SyncExample.RunExamples(console, args));
		}

		if (args.asyncExamples.Count > 0)
		{
			results.AddRange(AsyncExample.RunExamples(console, args));
		}

		return results;
	}

	private static int PrintSummary(List<ExampleResultInfo> results)
	{
		int passed = 0;
		int skipped = 0;
		int failed = 0;
		var nonPassed = new List<ExampleResultInfo>();

		foreach (ExampleResultInfo result in results)
		{
			switch (result.Result)
			{
				case ExampleResult.Passed:
					passed++;
					break;
				case ExampleResult.Skipped:
					skipped++;
					nonPassed.Add(result);
					break;
				case ExampleResult.Failed:
					failed++;
					nonPassed.Add(result);
					break;
			}
		}

		System.Console.WriteLine();
		System.Console.WriteLine($"Results: {passed} passed, {skipped} skipped, {failed} failed");

		foreach (ExampleResultInfo result in nonPassed)
		{
			string label = result.Result == ExampleResult.Skipped ? "SKIPPED" : "FAILED";
			System.Console.WriteLine($"  {label}: {result.Name} - {result.Message}");
		}

		System.Console.WriteLine();

		return failed > 0 ? 1 : 0;
	}

	private static void PrintUsage()
	{
		System.Console.WriteLine(
			"Usage: AerospikeExample [<options>] all|(<example1> <example2> ...)\n\n" +
			"Options:\n" +
			"  -h,  --host <host>            Seed hostname (default: 127.0.0.1)\n" +
			"                                May also be specified as:\n" +
			"                                  host1[:tlsname][:port1],...\n" +
			"  -p,  --port <port>            Server default port (default: 3000)\n" +
			"  -U,  --user <user>            User name\n" +
			"  -P,  --password <password>    Password\n" +
			"  -n,  --namespace <ns>         Namespace (default: test)\n" +
			"  -s,  --set <set>              Set name. Use 'empty' for empty set (default: demoset)\n" +
			"  -c,  --clusterName <name>     Expected cluster name\n" +
			"       --tls                    Enable TLS/SSL\n" +
			"       --tlsName <name>         TLS name\n" +
			"       --tlsProtocols <p>       TLS protocols (e.g. TLSv1.2)\n" +
			"       --tlsRevoke <list>       Revoke certificates by serial number\n" +
			"       --tlsClientCertFile <f>  TLS client certificate file\n" +
			"       --tlsLoginOnly           Use TLS on login only\n" +
			"       --auth <mode>            Authentication mode: " + string.Join(", ", Enum.GetNames(typeof(AuthMode))) + "\n" +
			"       --useServicesAlternate   Use services-alternate for cluster discovery\n" +
			"       --commandMax <n>         Max async commands in process (default: 40)\n" +
			"       --settings <path>        Load configuration from a .runsettings file\n" +
			"  -d,  --debug                  Run in debug mode\n" +
			"  -u,  --usage, --help          Print usage\n\n" +
			"Examples:\n"
		);

		foreach (string name in ExampleNames)
		{
			System.Console.WriteLine("  " + name);
		}

		System.Console.WriteLine();
		System.Console.WriteLine("All examples will be run if 'all' is specified.");
	}
}
