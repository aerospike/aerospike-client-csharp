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

public class QueryOpsProjection(Console console) : SyncExample(console)
{

	private const string KeyPrefix = "qopkey";

	/// <summary>
	/// Query operations projection.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RequireMinServerVersion(args, Node.SERVER_VERSION_8_1_2);

		WriteTestRecords(client, args);
		SimpleBinProjection(client, args);
		ExpressionBasedBinProjection(client, args);

		var verifyKey = new Key(args.ns, args.set, KeyPrefix + 3);
		Record verifyRec = client.Get(null, verifyKey)
			?? throw new Exception("QueryOpsProjection verification: record " + KeyPrefix + "3 not found.");

		object val1 = verifyRec.GetValue("test-bin-1");
		if (val1 is not string s1 || s1 != "value-1-3")
		{
			throw new Exception($"QueryOpsProjection verification: expected test-bin-1 'value-1-3', got {val1}.");
		}

		object counterVal = verifyRec.GetValue("counter");
		if (counterVal is not long counterLong || counterLong != 150L)
		{
			throw new Exception($"QueryOpsProjection verification: expected counter 150, got {counterVal}.");
		}

		console.Info("QueryOpsProjection verified successfully.");
	}

	private void WriteTestRecords(IAerospikeClient client, Arguments args)
	{
		console.Info("Write test records for query ops projection.");

		for (int i = 1; i <= 5; i++)
		{
			var key = new Key(args.ns, args.set, KeyPrefix + i);
			var mapData = new Dictionary<string, string>
			{
				{ "a", "map-val-" + i },
				{ "b", "other-" + i }
			};

			client.Put(args.writePolicy, key,
				new Bin("test-bin-1", "value-1-" + i),
				new Bin("test-bin-2", "value-2-" + i),
				new Bin("test-map-bin", mapData),
				new Bin("counter", i * 50)
			);

			console.Info($"Put: ns={key.ns} set={key.setName} key={key.userKey}");
		}
	}

	private void SimpleBinProjection(IAerospikeClient client, Arguments args)
	{
		// Create statement (no filter = scan all records)
		var statement = new Statement
		{
			Namespace = args.ns,
			SetName = args.set,
			// Define read operations for projection
			Operations =
			[
				Operation.Get("test-bin-1"),
				Operation.Get("test-bin-2"),
				MapOperation.GetByKey("test-map-bin", Value.Get("a"), MapReturnType.VALUE)
			]
		};

		// Execute query with operations
		using RecordSet recordSet = client.Query(null, statement);

		while (recordSet.Next())
		{
			var record = recordSet.Record;
			var key = recordSet.Key;

			console.Info($"Key: {key.userKey}");
			console.Info($"  test-bin-1: {record.GetValue("test-bin-1")}");
			console.Info($"  test-bin-2: {record.GetValue("test-bin-2")}");
			console.Info($"  test-map-bin: {record.GetValue("test-map-bin")}");
		}
	}

	private void ExpressionBasedBinProjection(IAerospikeClient client, Arguments args)
	{
		// Create statement (no filter = scan all records)
		var statement = new Statement
		{
			Namespace = args.ns,
			SetName = args.set,
			// Create expression operations for computed projections
			Operations =
			[
				// Read the original bin value
				ExpOperation.Read(
					"original-value",
					Exp.Build(Exp.IntBin("counter")),
					ExpReadFlags.DEFAULT
				),
			
				// Compute doubled value
				ExpOperation.Read(
					"doubled-value",
					Exp.Build(
						Exp.Mul(
							Exp.IntBin("counter"),
							Exp.Val(2)
						)
					),
					ExpReadFlags.DEFAULT
				),
			
				// Compute a conditional value
				ExpOperation.Read(
					"status",
					Exp.Build(
						Exp.Cond(
							Exp.GE(Exp.IntBin("counter"), Exp.Val(100)),
							Exp.Val("high"),
							Exp.Val("low")
						)
					),
					ExpReadFlags.DEFAULT
				)
			]
		};

		// Execute query with expression operations
		using RecordSet recordSet = client.Query(null, statement);

		while (recordSet.Next())
		{
			var record = recordSet.Record;
			console.Info($"Original: {record.GetValue("original-value")}");
			console.Info($"Doubled: {record.GetValue("doubled-value")}");
			console.Info($"Status: {record.GetValue("status")}");
		}
	}
}
