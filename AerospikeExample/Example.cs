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

namespace Aerospike.Example;

/// <summary>
/// Thrown by examples that cannot run against the current server configuration.
/// Analogous to Assert.Inconclusive in the test suite.
/// </summary>
public class ExampleSkipException(string reason) : Exception(reason)
{
}

public enum ExampleResult
{
	Passed,
	Skipped,
	Failed
}

public class ExampleResultInfo(string name, ExampleResult result, string message = null)
{
	public string Name { get; } = name;
	public ExampleResult Result { get; } = result;
	public string Message { get; } = message;
}

public abstract class Example(Console console)
{
	protected internal Console console = console;
	public volatile bool valid;

	public void Stop()
	{
		valid = false;
	}

	public ExampleResultInfo Run(Arguments args)
	{
		string name = GetType().Name;
		valid = true;

		try
		{
			console.Info($"{name} Begin");
			RunExample(args);
			console.Info($"{name} End");
			return new ExampleResultInfo(name, ExampleResult.Passed);
		}
		catch (ExampleSkipException ex)
		{
			console.Warn($"{name} SKIPPED: {ex.Message}");
			return new ExampleResultInfo(name, ExampleResult.Skipped, ex.Message);
		}
	}

	public abstract void RunExample(Arguments args);

	protected static void SkipUnless(bool condition, string reason)
	{
		if (!condition)
		{
			throw new ExampleSkipException(reason);
		}
	}

	protected static void RequireEnterprise(Arguments args)
	{
		SkipUnless(args.enterprise, "requires Enterprise edition");
	}

	protected static void RequireMinServerVersion(Arguments args, Version version)
	{
		SkipUnless(args.serverVersion >= version, $"requires server version {version} or later");
	}

	protected static void RequireStrongConsistency(Arguments args)
	{
		SkipUnless(args.scMode, "requires strong consistency mode");
	}

	// Used in Connect examples
	protected static void RequireBasic(Arguments args)
	{
		SkipUnless(!args.useServicesAlternate, "requires basic mode");
		SkipUnless(args.user == null, "requires no authentication");
		SkipUnless(args.tlsPolicy == null, "requires TLS disabled");
	}

	protected static void RequireAuth(Arguments args)
	{
		SkipUnless(args.user != null, "requires authentication");
	}

	protected static void RequireTls(Arguments args)
	{
		SkipUnless(args.tlsPolicy != null, "requires TLS");
	}

	protected static void RequirePki(Arguments args)
	{
		SkipUnless(args.authMode == Aerospike.Client.AuthMode.PKI, "requires PKI authentication");
	}
}
