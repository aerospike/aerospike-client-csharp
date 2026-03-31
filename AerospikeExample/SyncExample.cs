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

namespace Aerospike.Example
{
	public abstract class SyncExample(Console console) : Example(console)
	{

		/// <summary>
		/// Connect and run one or more synchronous client examples, sharing a single client connection.
		/// </summary>
		public static List<ExampleResultInfo> RunExamples(Console console, Arguments args)
		{
			ClientPolicy policy = new()
			{
				user = args.user,
				password = args.password,
				clusterName = args.clusterName,
				tlsPolicy = args.tlsPolicy,
				authMode = args.authMode,
				useServicesAlternate = args.useServicesAlternate
			};

			args.writePolicy = policy.writePolicyDefault;
			args.policy = policy.readPolicyDefault;
			args.batchPolicy = policy.batchPolicyDefault;

			AerospikeClient client = new(policy, args.hosts);
			List<ExampleResultInfo> results = [];

			try
			{
				args.SetServerSpecific(client);

				foreach (string exampleName in args.syncExamples)
				{
					results.Add(RunExample(exampleName, client, args, console));
				}
			}
			finally
			{
				client.Close();
			}

			return results;
		}

		private static ExampleResultInfo RunExample(string exampleName, IAerospikeClient client, Arguments args, Console console)
		{
			string fullName = "Aerospike.Example." + exampleName;
			Type type = Type.GetType(fullName);

			if (type == null || !typeof(SyncExample).IsAssignableFrom(type))
			{
				console.Error($"Invalid sync example: {exampleName}");
				return new ExampleResultInfo(exampleName, ExampleResult.Failed, "example class not found");
			}

			SyncExample example = (SyncExample)Activator.CreateInstance(type, console);

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

		public ExampleResultInfo Run(IAerospikeClient client, Arguments args)
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
			throw new NotSupportedException("Use RunExamples() to run sync examples via the console runner.");
		}

		public abstract void RunExample(IAerospikeClient client, Arguments args);
	}
}
