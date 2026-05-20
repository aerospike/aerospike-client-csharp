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
using System.Globalization;
using System.Xml.Linq;

namespace Aerospike.Example;

public static class Program
{
	public static int Main(string[] args)
	{
		try
		{
			Arguments arguments = ParseArguments(args);

			if (arguments == null)
			{
				return 1;
			}

			if (arguments.syncExamples.Count == 0 && arguments.asyncExamples.Count == 0)
			{
				PrintUsage();
				return 1;
			}

			Console console = new();
			List<ExampleResultInfo> results = RunExamples(console, arguments);
			if (!string.IsNullOrEmpty(arguments.reportTrxPath))
			{
				WriteTrxReport(arguments.reportTrxPath, results);
			}

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

		// Defaults.
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
		string reportTrxPath = null;
		List<string> exampleNames = [];
		HashSet<string> cliOverrides = new(StringComparer.OrdinalIgnoreCase);

		for (int i = 0; i < args.Length; i++)
		{
			string arg = args[i];

			switch (arg)
			{
				case "-h":
				case "--host":
					if (!TryNext(args, ref i, out host)) { PrintUsage(); return null; }
					cliOverrides.Add("Host");
					break;

				case "-p":
				case "--port":
					if (!TryNext(args, ref i, out string portText) || !int.TryParse(portText, NumberStyles.Integer, CultureInfo.InvariantCulture, out port))
					{
						PrintUsage(); return null;
					}
					cliOverrides.Add("Port");
					break;

				case "-U":
				case "--user":
					if (!TryNext(args, ref i, out user)) { PrintUsage(); return null; }
					cliOverrides.Add("User");
					break;

				case "-P":
				case "--password":
					if (!TryNext(args, ref i, out password)) { PrintUsage(); return null; }
					cliOverrides.Add("Password");
					break;

				case "-n":
				case "--namespace":
					if (!TryNext(args, ref i, out ns)) { PrintUsage(); return null; }
					cliOverrides.Add("Namespace");
					break;

				case "-s":
				case "--set":
					if (!TryNext(args, ref i, out set)) { PrintUsage(); return null; }
					if (set.Equals("empty", StringComparison.OrdinalIgnoreCase))
					{
						set = string.Empty;
					}
					cliOverrides.Add("Set");
					break;

				case "-c":
				case "--clusterName":
					if (!TryNext(args, ref i, out clusterName)) { PrintUsage(); return null; }
					cliOverrides.Add("ClusterName");
					break;

				case "--tls":
				case "--tlsEnable":
					tlsEnable = true;
					cliOverrides.Add("TlsEnable");
					break;

				case "--tlsName":
					if (!TryNext(args, ref i, out tlsName)) { PrintUsage(); return null; }
					cliOverrides.Add("TlsName");
					break;

				case "--tlsProtocols":
					if (!TryNext(args, ref i, out tlsProtocols)) { PrintUsage(); return null; }
					cliOverrides.Add("TlsProtocols");
					break;

				case "--tlsRevoke":
					if (!TryNext(args, ref i, out tlsRevoke)) { PrintUsage(); return null; }
					cliOverrides.Add("TlsRevoke");
					break;

				case "--tlsClientCertFile":
					if (!TryNext(args, ref i, out tlsClientCertFile)) { PrintUsage(); return null; }
					cliOverrides.Add("TlsClientCertFile");
					break;

				case "--tlsLoginOnly":
					tlsLoginOnly = true;
					cliOverrides.Add("TlsLoginOnly");
					break;

				case "--auth":
					if (!TryNext(args, ref i, out string authText) || !Enum.TryParse(authText, ignoreCase: true, out authMode))
					{
						PrintUsage(); return null;
					}
					cliOverrides.Add("AuthMode");
					break;

				case "--useServicesAlternate":
					useServicesAlternate = true;
					cliOverrides.Add("UseServicesAlternate");
					break;

				case "--commandMax":
					if (!TryNext(args, ref i, out string commandMaxText) || !int.TryParse(commandMaxText, NumberStyles.Integer, CultureInfo.InvariantCulture, out commandMax))
					{
						PrintUsage(); return null;
					}
					break;

				case "--settings":
					if (!TryNext(args, ref i, out settingsPath)) { PrintUsage(); return null; }
					break;

				case "--report-trx":
					if (!TryNext(args, ref i, out reportTrxPath)) { PrintUsage(); return null; }
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
		}

		// .runsettings values fill in anything the CLI didn't override.
		if (settingsPath != null)
		{
			Dictionary<string, string> settings = LoadRunSettings(settingsPath);

			void Apply(string key, Action<string> setter)
			{
				if (!cliOverrides.Contains(key) &&
					settings.TryGetValue(key, out string value) &&
					!string.IsNullOrEmpty(value))
				{
					setter(value);
				}
			}

			Apply("Host", v => host = v);
			Apply("Port", v => port = int.Parse(v, CultureInfo.InvariantCulture));
			Apply("Namespace", v => ns = v);
			Apply("Set", v => set = v);
			Apply("User", v => user = v);
			Apply("Password", v => password = v);
			Apply("ClusterName", v => clusterName = v);
			Apply("AuthMode", v => authMode = Enum.Parse<AuthMode>(v, ignoreCase: true));
			Apply("UseServicesAlternate", v => useServicesAlternate = bool.Parse(v));
			Apply("TlsEnable", v => tlsEnable = bool.Parse(v));
			Apply("TlsName", v => tlsName = v);
			Apply("TlsProtocols", v => tlsProtocols = v);
			Apply("TlsRevoke", v => tlsRevoke = v);
			Apply("TlsClientCertFile", v => tlsClientCertFile = v);
			Apply("TlsLoginOnly", v => tlsLoginOnly = bool.Parse(v));
		}

		// Expand "all".
		if (exampleNames.Any(name => name.Equals("all", StringComparison.OrdinalIgnoreCase)))
		{
			exampleNames = [.. ExampleRegistry.Names];
		}

		TlsPolicy tlsPolicy = tlsEnable
			? new TlsPolicy(tlsProtocols, tlsRevoke, tlsClientCertFile, tlsLoginOnly)
			: null;

		Log.SetLevel(debug ? Log.Level.DEBUG : Log.Level.INFO);

		Arguments arguments = new()
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
			commandMax = commandMax,
			reportTrxPath = reportTrxPath
		};

		// Route each requested example to the correct runner based on registry metadata.
		foreach (string name in exampleNames)
		{
			if (ExampleRegistry.TryGet(name, out ExampleDefinition definition))
			{
				if (definition.IsAsync)
				{
					arguments.asyncExamples.Add(definition.Name);
				}
				else
				{
					arguments.syncExamples.Add(definition.Name);
				}
			}
			else
			{
				System.Console.Error.WriteLine($"Unknown example: {name}");
				return null;
			}
		}

		return arguments;
	}

	private static bool TryNext(string[] args, ref int i, out string value)
	{
		if (++i >= args.Length)
		{
			value = null;
			return false;
		}

		value = args[i];
		return true;
	}

	/// <summary>
	/// Parse a .runsettings XML file and extract TestRunParameters as a dictionary.
	/// </summary>
	private static Dictionary<string, string> LoadRunSettings(string path)
	{
		if (!File.Exists(path))
		{
			throw new FileNotFoundException($"Settings file not found: {path}");
		}

		Dictionary<string, string> settings = new(StringComparer.OrdinalIgnoreCase);
		XDocument doc = XDocument.Load(path);
		XElement testRunParams = doc.Descendants("TestRunParameters").FirstOrDefault();

		if (testRunParams == null)
		{
			return settings;
		}

		foreach (XElement param in testRunParams.Elements("Parameter"))
		{
			string name = param.Attribute("name")?.Value;
			string value = param.Attribute("value")?.Value;

			if (name != null)
			{
				settings[name] = value ?? string.Empty;
			}
		}

		return settings;
	}

	private static List<ExampleResultInfo> RunExamples(Console console, Arguments args)
	{
		List<ExampleResultInfo> results = [];

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
		List<ExampleResultInfo> nonPassed = [];

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

	private static void WriteTrxReport(string path, List<ExampleResultInfo> results)
	{
		string fullPath = Path.GetFullPath(path);
		string directory = Path.GetDirectoryName(fullPath);
		if (!string.IsNullOrEmpty(directory))
		{
			Directory.CreateDirectory(directory);
		}

		XNamespace ns = "http://microsoft.com/schemas/VisualStudio/TeamTest/2010";
		DateTimeOffset finish = DateTimeOffset.UtcNow;
		TimeSpan totalDuration = TimeSpan.FromTicks(results.Sum(result => result.Duration.Ticks));
		DateTimeOffset start = finish - totalDuration;
		Guid runId = Guid.NewGuid();
		Guid resultsListId = Guid.Parse("19431567-8539-422a-85d7-44ee4e166bda");
		Guid allResultsListId = Guid.Parse("8c43106b-9dc1-4907-a29f-aa66a61bf5b6");
		var testCases = results.Select(result => new
		{
			Result = result,
			TestId = Guid.NewGuid(),
			ExecutionId = Guid.NewGuid()
		}).ToList();

		XElement testRun = new(ns + "TestRun",
			new XAttribute("id", runId),
			new XAttribute("name", $"AerospikeExample {finish:yyyy-MM-dd HH:mm:ss}"),
			new XAttribute("runUser", Environment.UserName),
			new XElement(ns + "Times",
				new XAttribute("creation", ToTrxDate(start)),
				new XAttribute("queuing", ToTrxDate(start)),
				new XAttribute("start", ToTrxDate(start)),
				new XAttribute("finish", ToTrxDate(finish))),
			new XElement(ns + "Results",
				testCases.Select(testCase => CreateUnitTestResult(ns, testCase.Result, testCase.TestId, testCase.ExecutionId, finish))),
			new XElement(ns + "TestDefinitions",
				testCases.Select(testCase => new XElement(ns + "UnitTest",
					new XAttribute("name", testCase.Result.Name),
					new XAttribute("storage", "AerospikeExample.dll"),
					new XAttribute("id", testCase.TestId),
					new XElement(ns + "Execution", new XAttribute("id", testCase.ExecutionId)),
					new XElement(ns + "TestMethod",
						new XAttribute("codeBase", "AerospikeExample.dll"),
						new XAttribute("adapterTypeName", "executor://aerospike.example"),
						new XAttribute("className", "Aerospike.Example"),
						new XAttribute("name", testCase.Result.Name))))),
			new XElement(ns + "TestLists",
				new XElement(ns + "TestList",
					new XAttribute("name", "Results Not in a List"),
					new XAttribute("id", resultsListId)),
				new XElement(ns + "TestList",
					new XAttribute("name", "All Loaded Results"),
					new XAttribute("id", allResultsListId))),
			new XElement(ns + "TestEntries",
				testCases.Select(testCase => new XElement(ns + "TestEntry",
					new XAttribute("testId", testCase.TestId),
					new XAttribute("executionId", testCase.ExecutionId),
					new XAttribute("testListId", resultsListId)))),
			new XElement(ns + "ResultSummary",
				new XAttribute("outcome", results.Any(result => result.Result == ExampleResult.Failed) ? "Failed" : "Completed"),
				CreateCounters(ns, results)));

		new XDocument(new XDeclaration("1.0", "utf-8", "no"), testRun)
			.Save(fullPath, SaveOptions.None);

		System.Console.WriteLine($"TRX report written to {fullPath}");
	}

	private static XElement CreateUnitTestResult(XNamespace ns, ExampleResultInfo result, Guid testId, Guid executionId, DateTimeOffset finish)
	{
		DateTimeOffset start = finish - result.Duration;
		XElement element = new(ns + "UnitTestResult",
			new XAttribute("executionId", executionId),
			new XAttribute("testId", testId),
			new XAttribute("testName", result.Name),
			new XAttribute("computerName", Environment.MachineName),
			new XAttribute("duration", result.Duration.ToString(@"hh\:mm\:ss\.fffffff", CultureInfo.InvariantCulture)),
			new XAttribute("startTime", ToTrxDate(start)),
			new XAttribute("endTime", ToTrxDate(finish)),
			new XAttribute("outcome", ToTrxOutcome(result.Result)));

		if (!string.IsNullOrEmpty(result.Message))
		{
			element.Add(new XElement(ns + "Output",
				new XElement(ns + "ErrorInfo",
					new XElement(ns + "Message", result.Message))));
		}

		return element;
	}

	private static XElement CreateCounters(XNamespace ns, List<ExampleResultInfo> results)
	{
		int passed = results.Count(result => result.Result == ExampleResult.Passed);
		int failed = results.Count(result => result.Result == ExampleResult.Failed);
		int skipped = results.Count(result => result.Result == ExampleResult.Skipped);

		return new XElement(ns + "Counters",
			new XAttribute("total", results.Count),
			new XAttribute("executed", passed + failed),
			new XAttribute("passed", passed),
			new XAttribute("failed", failed),
			new XAttribute("error", 0),
			new XAttribute("timeout", 0),
			new XAttribute("aborted", 0),
			new XAttribute("inconclusive", 0),
			new XAttribute("passedButRunAborted", 0),
			new XAttribute("notRunnable", 0),
			new XAttribute("notExecuted", skipped),
			new XAttribute("disconnected", 0),
			new XAttribute("warning", 0),
			new XAttribute("completed", passed + failed),
			new XAttribute("inProgress", 0),
			new XAttribute("pending", 0));
	}

	private static string ToTrxOutcome(ExampleResult result) => result switch
	{
		ExampleResult.Passed => "Passed",
		ExampleResult.Skipped => "NotExecuted",
		ExampleResult.Failed => "Failed",
		_ => "Error"
	};

	private static string ToTrxDate(DateTimeOffset value)
		=> value.UtcDateTime.ToString("o", CultureInfo.InvariantCulture);

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
			$"       --auth <mode>            Authentication mode: {string.Join(", ", Enum.GetNames<AuthMode>())}\n" +
			"       --useServicesAlternate   Use services-alternate for cluster discovery\n" +
			"       --commandMax <n>         Max async commands in process (default: 40)\n" +
			"       --settings <path>        Load configuration from a .runsettings file\n" +
			"       --report-trx <path>      Write example results to a TRX report\n" +
			"  -d,  --debug                  Run in debug mode\n" +
			"  -u,  --usage, --help          Print usage\n\n" +
			"Examples:\n");

		foreach (string name in ExampleRegistry.Names)
		{
			System.Console.WriteLine($"  {name}");
		}

		System.Console.WriteLine();
		System.Console.WriteLine("All examples will be run if 'all' is specified.");
	}
}
