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
using System.Diagnostics;

namespace Aerospike.Example;

/// <summary>
/// Thrown by examples that cannot run against the current server configuration.
/// Analogous to Assert.Inconclusive in the test suite.
/// </summary>
public sealed class ExampleSkipException(string reason) : Exception(reason);

public enum ExampleResult
{
	Passed,
	Skipped,
	Failed
}

public sealed record ExampleResultInfo(string Name, ExampleResult Result, string Message = null, TimeSpan Duration = default);

public abstract class Example
{
	protected internal Console console;

	/// <summary>
	/// Connection and policy configuration injected by the harness.
	/// </summary>
	protected internal Arguments args { get; internal set; }

	protected string ns => args.ns;
	protected string set => args.set;
	protected Policy policy => args.policy;
	protected WritePolicy writePolicy => args.writePolicy;
	protected BatchPolicy batchPolicy => args.batchPolicy;

	internal void SetConsole(Console console)
	{
		this.console = console;
	}

	public abstract void RunExample(Arguments args);

	protected ExampleResultInfo RunWithResult(Action run)
	{
		string name = GetType().Name;
		int errorCount = console.ErrorCount;
		Stopwatch stopwatch = Stopwatch.StartNew();

		try
		{
			console.Info($"{name} Begin");
			run();
			console.Info($"{name} End");

			if (console.ErrorCount > errorCount)
			{
				return new ExampleResultInfo(name, ExampleResult.Failed, "example logged one or more errors", stopwatch.Elapsed);
			}

			return new ExampleResultInfo(name, ExampleResult.Passed, Duration: stopwatch.Elapsed);
		}
		catch (ExampleSkipException ex)
		{
			console.Warn($"{name} SKIPPED: {ex.Message}");
			return new ExampleResultInfo(name, ExampleResult.Skipped, ex.Message, stopwatch.Elapsed);
		}
		catch (Exception ex)
		{
			console.Error($"{name} FAILED: {ex.Message}");
			return new ExampleResultInfo(name, ExampleResult.Failed, ex.Message, stopwatch.Elapsed);
		}
	}

	protected static void SkipUnless(bool condition, string reason)
	{
		if (!condition)
		{
			throw new ExampleSkipException(reason);
		}
	}

	protected void RequireEnterprise()
	{
		SkipUnless(args.enterprise, "requires Enterprise edition");
	}

	protected void RequireMinServerVersion(Version version)
	{
		SkipUnless(args.serverVersion >= version, $"requires server version {version} or later");
	}

	protected void RequireStrongConsistency()
	{
		SkipUnless(args.scMode, "requires strong consistency mode");
	}

	// Used in Connect examples that build their own client.
	protected void RequireBasic()
	{
		SkipUnless(!args.useServicesAlternate, "requires basic mode");
		SkipUnless(args.user == null, "requires no authentication");
		SkipUnless(args.tlsPolicy == null, "requires TLS disabled");
	}

	protected void RequireAuth()
	{
		SkipUnless(args.user != null, "requires authentication");
	}

	protected void RequireTls()
	{
		SkipUnless(args.tlsPolicy != null, "requires TLS");
	}

	protected void RequirePki()
	{
		SkipUnless(args.authMode == AuthMode.PKI, "requires PKI authentication");
	}
}
