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

public abstract class AsyncExample : Example
{
	protected AsyncClient client { get; private set; }

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

		using AsyncClient client = new(policy, args.hosts);

		args.writePolicy = policy.writePolicyDefault;
		args.policy = policy.readPolicyDefault;
		args.batchPolicy = policy.batchPolicyDefault;
		args.SetServerSpecific(client);

		List<ExampleResultInfo> results = [];

		foreach (string exampleName in args.asyncExamples)
		{
			results.Add(RunExample(exampleName, client, args, console));
		}

		return results;
	}

	private static ExampleResultInfo RunExample(string exampleName, AsyncClient client, Arguments args, Console console)
	{
		if (!ExampleRegistry.TryGetAsync(exampleName, out ExampleDefinition definition))
		{
			console.Error($"Invalid async example: {exampleName}");
			return new ExampleResultInfo(exampleName, ExampleResult.Failed, "example class not found");
		}

		AsyncExample example = (AsyncExample)Activator.CreateInstance(definition.Type);
		example.SetConsole(console);

		try
		{
			return example.Run(client, args, definition.Fixture);
		}
		catch (Exception ex)
		{
			console.Error($"{exampleName} FAILED: {ex.Message}");
			return new ExampleResultInfo(exampleName, ExampleResult.Failed, ex.Message);
		}
	}

	internal ExampleResultInfo Run(AsyncClient client, Arguments args, ExampleFixture fixture)
	{
		this.client = client;
		this.args = args;

		return RunWithResult(() =>
		{
			try
			{
				fixture?.Setup?.Invoke(client, args);
				RunExample(client, args);
				fixture?.Validate?.Invoke(client, args);
			}
			finally
			{
				fixture?.Cleanup?.Invoke(client, args);
			}
		});
	}

	public override void RunExample(Arguments args)
	{
		throw new NotSupportedException("Use RunExamples() to run async examples via the console runner.");
	}

	public virtual void RunExample(AsyncClient client, Arguments args)
	{
		RunExample();
	}

	public virtual void RunExample()
	{
		throw new NotSupportedException("Override RunExample() or RunExample(client, args).");
	}
}
