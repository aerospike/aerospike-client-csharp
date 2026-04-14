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

/// <summary>
/// Demonstrate read patterns used in documentation snippets.
/// Each snippet region is validated at runtime against a real server.
/// </summary>
public class Get(Console console) : SyncExample(console)
{
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		SetupRecord(client, args);
		RunReadPolicy();
		RunExists(client, args);
		RunGetHeader(client, args);
		RunGetWholeRecord(client, args);
		RunGetSpecificBins(client, args);
		Cleanup(client, args);
		console.Info("Read completed successfully.");
	}

	private static void SetupRecord(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "docreadkey");
		Bin report = new Bin("report", "sample-report");
		Bin location = new Bin("location", "sample-location");
		client.Put(args.writePolicy, key, report, location);
	}

	private static void Cleanup(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "docreadkey");
		client.Delete(args.writePolicy, key);
	}

	private void RunReadPolicy()
	{
		// @@@SNIPSTART csharp-client-read-policy
		// Create new read policy
		Policy policy = new()
		{
			socketTimeout = 300
		};
		// @@@SNIPEND

		console.Info("Read policy created: socketTimeout={0}", policy.socketTimeout);
	}

	private void RunExists(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "docreadkey");
		Policy policy = args.policy;

		// @@@SNIPSTART csharp-client-read-exists
		// Returns true if exists, false if not
		bool exists = client.Exists(policy, key);
		// @@@SNIPEND

		if (!exists)
		{
			throw new Exception("Exists check failed: record should exist.");
		}
		console.Info("Exists: {0}", exists);
	}

	private void RunGetHeader(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "docreadkey");
		Policy policy = args.policy;

		// @@@SNIPSTART csharp-client-read-metadata
		// Get record metadata
		Record record = client.GetHeader(policy, key);

		// Do something
		System.Console.WriteLine("Record: {0}", record);
		// @@@SNIPEND

		if (record == null)
		{
			throw new Exception("GetHeader failed: record not found.");
		}
		console.Info("GetHeader: generation={0} expiration={1}", record.generation, record.expiration);
	}

	private void RunGetWholeRecord(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "docreadkey");
		Policy policy = args.policy;

		// @@@SNIPSTART csharp-client-read-whole-record
		// Get whole record
		Record record = client.Get(policy, key);

		// Do something
		System.Console.WriteLine("Record: {0}", record.ToString().Split("bins:")[1]);
		// @@@SNIPEND

		if (record == null)
		{
			throw new Exception("Get failed: record not found.");
		}
		console.Info("Get: {0}", record);
	}

	private void RunGetSpecificBins(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "docreadkey");
		Policy policy = args.policy;

		// @@@SNIPSTART csharp-client-read-specific-bins
		// Get bins 'report' and 'location'
		Record record = client.Get(policy, key, "report", "location");

		// Do something
		System.Console.WriteLine("Record: {0}", record.ToString().Split("bins:")[1])
		// @@@SNIPEND

		if (record == null)
		{
			throw new Exception("Get specific bins failed: record not found.");
		}
		console.Info("Get bins: {0}", record);
	}

	/// <summary>
	/// Compile-validated documentation snippets for the read page.
	/// These methods are never called at runtime; they exist solely
	/// so the compiler verifies the example code shown in docs.
	/// </summary>
	public static void Setup()
	{
		// @@@SNIPSTART csharp-client-read-setup
		// Define host configuration
		Host config = new("127.0.0.1", 3000);
		// Establishes a connection to the server
		AerospikeClient client = new(null, config);

		// Creates a key with the namespace "sandbox", set "ufodata", and user key 5001
		Key key = new("sandbox", "ufodata", 5001);
		// @@@SNIPEND
	}

	public static void FullExampleReadRecord()
	{
		// @@@SNIPSTART csharp-client-read-full
		// Define host configuration
		Host config = new("127.0.0.1", 3000);
		// Establishes a connection to the server
		AerospikeClient client = new(null, config);

		// Creates a key with the namespace "sandbox", set "ufodata", and user key 5001
		Key key = new("sandbox", "ufodata", 5001);

		// Create new read policy
		Policy policy = new()
		{
			socketTimeout = 300
		};

		// Get whole record
		Record record = client.Get(policy, key);

		// Do something
		System.Console.WriteLine("Record: {0}", record.ToString().Split("bins:")[1]);

		// Close the connection to the server
		client.Close();
		//testing 123
		// @@@SNIPEND
	}
}
