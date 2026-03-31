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

namespace Aerospike.Example;

public abstract class AsyncExample(Console console) : Example(console)
{

	/// <summary>
	/// Connect and run one or more asynchronous client examples, sharing a single client connection.
	/// </summary>
	public static List<ExampleResultInfo> RunExamples(Console console, Arguments args)
	{
		AsyncClientPolicy policy = new()
		{
			user = args.user,
			password = args.password,
			clusterName = args.clusterName,
			tlsPolicy = args.tlsPolicy,
			authMode = args.authMode,
			asyncMaxCommands = args.commandMax,
			useServicesAlternate = args.useServicesAlternate,
			failIfNotConnected = true
		};

		args.writePolicy = policy.writePolicyDefault;
		args.policy = policy.readPolicyDefault;
		args.batchPolicy = policy.batchPolicyDefault;

		using var client = new AsyncClient(policy, args.hosts);
		var results = new List<ExampleResultInfo>();

		args.SetServerSpecific(client);

		foreach (string exampleName in args.asyncExamples)
		{
			results.Add(RunExample(exampleName, client, args, console));
		}

		return results;
	}

	private static ExampleResultInfo RunExample(string exampleName, AsyncClient client, Arguments args, Console console)
	{
		string fullName = "Aerospike.Example." + exampleName;
		Type type = Type.GetType(fullName);

		if (type == null || !typeof(AsyncExample).IsAssignableFrom(type))
		{
			console.Error($"Invalid async example: {exampleName}");
			return new ExampleResultInfo(exampleName, ExampleResult.Failed, "example class not found");
		}

		AsyncExample example = (AsyncExample)Activator.CreateInstance(type, console);

		try
		{
			return example.Run(client, args);
		}
		catch (Exception ex)
		{
			console.Error($"{exampleName} FAILED: {ex.Message}");
			return new ExampleResultInfo(exampleName, ExampleResult.Failed, ex.Message);
		}
	}

	public ExampleResultInfo Run(AsyncClient client, Arguments args)
	{
		string name = GetType().Name;
		valid = true;

		try
		{
			console.Info($"{name} Begin");
			RunExample(client, args);
			console.Info($"{name} End");
			return new ExampleResultInfo(name, ExampleResult.Passed);
		}
		catch (ExampleSkipException ex)
		{
			console.Warn($"{name} SKIPPED: {ex.Message}");
			return new ExampleResultInfo(name, ExampleResult.Skipped, ex.Message);
		}
	}

	public override void RunExample(Arguments args)
	{
		AsyncClientPolicy policy = new()
		{
			user = args.user,
			password = args.password,
			clusterName = args.clusterName,
			tlsPolicy = args.tlsPolicy,
			authMode = args.authMode,
			asyncMaxCommands = args.commandMax,
			failIfNotConnected = true
		};

		using var client = new AsyncClient(policy, args.hosts);

		args.SetServerSpecific(client);
		RunExample(client, args);
	}

	public abstract void RunExample(AsyncClient client, Arguments args);
}
