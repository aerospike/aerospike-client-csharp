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
using System.Collections;

namespace Aerospike.Example;

internal static class ExampleStateValidation
{
	private const int ScanSeedBegin = 1;
	private const int ScanSeedEnd = 10;
	private const string ScanSeedKeyPrefix = "scankey";
	private const string ScanSeedBinName = "scanbin";

	public static void PutGet(IAerospikeClient client, Arguments args)
	{
		Key key = ExampleFixtureSupport.Key(args, "putgetkey");
		Record record = client.Get(args.policy, key, "bin1") ?? throw new Exception("PutGet verification failed: record not found for putgetkey.");
		object received = record.GetValue("bin1");

		if (!Equals(received, "value1"))
		{
			throw new Exception($"PutGet verification failed: expected bin1=value1, got {ExampleValueFormatter.Format(received)}.");
		}

		Record header = client.GetHeader(args.policy, key);

		if (header == null || header.generation == 0)
		{
			throw new Exception("PutGet verification failed: record header was not populated.");
		}
	}

	public static void Add(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "addkey", "addbin", 45L);
	}

	public static void Append(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "appendkey", "appendbin", "Hello World");
	}

	public static void Prepend(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "prependkey", "prependbin", "Hello World");
	}

	public static void SetupDelete(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.PutBins(client, args, "deletekey", new Bin("bin", "value"));
		ExampleFixtureSupport.PutBins(client, args, "durabledeletekey", new Bin("bin", "durablevalue"));
	}

	public static void Delete(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertNotExists(client, args, "deletekey");
		ExampleFixtureSupport.AssertNotExists(client, args, "durabledeletekey");
	}

	public static void Exists(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertNotExists(client, args, "existskey");
	}

	public static void SetupReplace(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.DeleteKeys(client, args, ["replacekey", "replaceonlykey"]);
		ExampleFixtureSupport.PutBins(
			client,
			args,
			"replacekey",
			new Bin("bin1", "value1"),
			new Bin("bin2", "value2"));
	}

	public static void Replace(IAerospikeClient client, Arguments args)
	{
		Record record = client.Get(args.policy, ExampleFixtureSupport.Key(args, "replacekey")) ?? throw new Exception("Replace verification failed: replacekey record not found.");
		if (record.GetValue("bin1") != null || record.GetValue("bin2") != null)
		{
			throw new Exception("Replace verification failed: bins removed by replace should be absent.");
		}

		if (!Equals(record.GetValue("bin3"), "value3"))
		{
			throw new Exception($"Replace verification failed: expected bin3=value3, got {ExampleValueFormatter.Format(record.GetValue("bin3"))}.");
		}

		ExampleFixtureSupport.AssertNotExists(client, args, "replaceonlykey");
	}

	public static void Batch(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 8; i++)
		{
			ExampleFixtureSupport.AssertBin(client, args, "batchkey" + i, "batchbin", "batchvalue" + i);
		}
	}

	public static void SetupBatch(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 8; i++)
		{
			ExampleFixtureSupport.PutBins(client, args, "batchkey" + i, new Bin("batchbin", "batchvalue" + i));
		}
	}

	public static void SetupBatchOperate(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 8; i++)
		{
			List<int> list = [];

			for (int j = 0; j < i; j++)
			{
				list.Add(j * i);
			}

			ExampleFixtureSupport.PutBins(
				client,
				args,
				"bkey" + i,
				new Bin("bin1", i),
				new Bin("bin2", i + 10),
				new Bin("bin3", list));
		}
	}

	public static void BatchOperate(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 8; i++)
		{
			Key key = ExampleFixtureSupport.Key(args, "bkey" + i);

			if (i == 6)
			{
				if (client.Exists(args.policy, key))
				{
					throw new Exception("BatchOperate verification failed: bkey6 should have been deleted.");
				}

				continue;
			}

			Record current = client.Get(args.policy, key) ?? throw new Exception($"BatchOperate verification failed: bkey{i} record not found.");
			IList currentList = current.GetList("bin3");

			if (currentList == null ||
				currentList.Count != i + 1 ||
				Convert.ToInt64(currentList[currentList.Count - 1]) != 999)
			{
				throw new Exception($"BatchOperate verification failed: bkey{i} bin3 list was not appended.");
			}
		}

		Record record = client.Get(args.policy, ExampleFixtureSupport.Key(args, "bkey2")) ?? throw new Exception("BatchOperate verification failed: bkey2 record not found.");
		if (record.GetInt("bin1") != 2 || record.GetInt("bin2") != 12)
		{
			throw new Exception("BatchOperate verification failed: bkey2 bin1/bin2 mismatch.");
		}

		IList list = record.GetList("bin3");

		if (list == null || list.Count != 3 ||
			Convert.ToInt64(list[0]) != 0 ||
			Convert.ToInt64(list[1]) != 2 ||
			Convert.ToInt64(list[2]) != 999)
		{
			throw new Exception("BatchOperate verification failed: bkey2 bin3 list mismatch.");
		}

		ExampleFixtureSupport.AssertBin(client, args, "bkey1", "bin4", 100L);
		ExampleFixtureSupport.AssertBin(client, args, "bkey4", "bin1", 1018L);
	}

	public static void Expire(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertNotExists(client, args, "expirekey");
	}

	public static void Generation(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "genkey", "genbin", "genvalue3");
	}

	public static void Touch(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertNotExists(client, args, "touchkey");
	}

	public static void Operate(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "opkey", "optintbin", 11L);
		ExampleFixtureSupport.AssertBin(client, args, "opkey", "optstringbin", "new string");
	}

	public static void SetupOperate(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.PutBins(
			client,
			args,
			"opkey",
			new Bin("optintbin", 7),
			new Bin("optstringbin", "string value"));
	}

	public static void SetupOperateBit(IAerospikeClient client, Arguments args)
	{
		byte[] bytes = [0x01, 0x02, 0x03, 0x04, 0x05];
		ExampleFixtureSupport.PutBins(client, args, "bitkey", new Bin("bitbin", bytes));
	}

	public static void OperateList(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "listkey", "listbin", new List<object> { 55L });
		ExampleFixtureSupport.AssertBin(client, args, "listkey2", "listbin", new List<object>
		{
			new List<object> { 7L, 9L, 5L },
			new List<object> { 1L, 2L, 3L },
			new List<object> { 6L, 5L, 4L, 1L, 11L }
		});
	}

	public static void OperateMap(IAerospikeClient client, Arguments args)
	{
		AssertMapValue(client, args, "mapkey_score", "mapbin", 2L, 33L);
		AssertMapValue(client, args, "mapkey_range", "mapbin", "John", 81L);
		AssertMapValue(client, args, "mapkey_range", "mapbin", "Jim", 94L);
		AssertMapValue(client, args, "mapkey_nested", "mapbin", ["key2", "key21"], 11L);
		AssertMapValue(client, args, "mapkey_nested_create", "mapbin", ["key2", "b"], 4L);
		AssertMapValue(client, args, "mapkey3", "mapbin", ["key2"], new List<object> { 1L, 2L });
		ExampleFixtureSupport.AssertBinExists(client, args, "mapkey", "mapbin");
	}

	public static void ListMap(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "listkey1", "listbin1", new List<object> { "string1", "string2", "string3" });
		ExampleFixtureSupport.AssertBin(client, args, "listkey2", "listbin2", new List<object> { "string1", 2L, new byte[] { 3, 52, 125 } });
		ExampleFixtureSupport.AssertBin(client, args, "mapkey1", "mapbin1", new Dictionary<object, object>
		{
			["key1"] = "string1",
			["key2"] = "string2",
			["key3"] = "string3"
		});
		ExampleFixtureSupport.AssertBin(client, args, "mapkey2", "mapbin2", new Dictionary<object, object>
		{
			["key1"] = "string1",
			["key2"] = 2L,
			["key3"] = new byte[] { 3, 52, 125 },
			["key4"] = new List<object> { 100034L, 12384955L, 3L, 512L }
		});
		ExampleFixtureSupport.AssertBin(client, args, "listmapkey", "listmapbin", new List<object>
		{
			"string1",
			8L,
			new List<object> { "string2", 5L },
			new Dictionary<object, object>
			{
				["a"] = 1L,
				[2L] = "b",
				[3L] = new byte[] { 3, 52, 125 },
				["list"] = new List<object> { "string2", 5L }
			}
		});
	}

	public static void OperateBit(IAerospikeClient client, Arguments args)
	{
		Record record = client.Get(args.policy, ExampleFixtureSupport.Key(args, "bitkey"), "bitbin") ?? throw new Exception("OperateBit verification failed: bitkey record not found.");
		byte[] bytes = (byte[])record.GetValue("bitbin");

		if (bytes == null || bytes.Length != 5 || bytes[4] != 0x07)
		{
			throw new Exception("OperateBit verification failed: expected last three bits to be set.");
		}
	}

	public static void DeleteBin(IAerospikeClient client, Arguments args)
	{
		Record record = client.Get(args.policy, ExampleFixtureSupport.Key(args, "delbinkey")) ?? throw new Exception("DeleteBin verification failed: delbinkey record not found.");
		if (record.GetValue("bin1") != null)
		{
			throw new Exception("DeleteBin verification failed: bin1 should be absent.");
		}

		if (!Equals(record.GetValue("bin2"), "value2"))
		{
			throw new Exception($"DeleteBin verification failed: expected bin2=value2, got {ExampleValueFormatter.Format(record.GetValue("bin2"))}.");
		}
	}

	public static void SetupDeleteBin(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.PutBins(
			client,
			args,
			"delbinkey",
			new Bin("bin1", "value1"),
			new Bin("bin2", "value2"));
	}

	public static void ScanDefaultSet(IAerospikeClient client, Arguments args)
	{
		HashSet<long> expectedValues = Enumerable
			.Range(ScanSeedBegin, ScanSeedEnd - ScanSeedBegin + 1)
			.Select(i => (long)i)
			.ToHashSet();

		ScanPolicy policy = new();
		client.ScanAll(policy, args.ns, args.set, (Key key, Record record) =>
		{
			object received = record.GetValue(ScanSeedBinName);

			if (received is IConvertible)
			{
				long value = Convert.ToInt64(received);

				if (value >= ScanSeedBegin && value <= ScanSeedEnd)
				{
					lock (expectedValues)
					{
						expectedValues.Remove(value);
					}
				}
			}
		});

		if (expectedValues.Count > 0)
		{
			throw new Exception($"Scan verification failed: seeded scanbin values were not scanned: {string.Join(", ", expectedValues)}.");
		}
	}

	public static void SetupScanDefaultSet(IAerospikeClient client, Arguments args)
	{
		for (int i = ScanSeedBegin; i <= ScanSeedEnd; i++)
		{
			ExampleFixtureSupport.PutBins(client, args, ScanSeedKeyPrefix + i, new Bin(ScanSeedBinName, i));
		}
	}

	public static void CleanupScanDefaultSet(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.DeleteRange(client, args, ScanSeedKeyPrefix, ScanSeedBegin, ScanSeedEnd);
	}

	public static void ScanPage(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, 1, "bin", 1, "page");
	}

	public static void SetupScanPage(IAerospikeClient client, Arguments args)
	{
		SetupIntegerRecords(client, args, "page", "bin", 1, 190);
	}

	public static void ScanResume(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, 1, "bin", 1, "resume");
	}

	public static void SetupScanResume(IAerospikeClient client, Arguments args)
	{
		SetupIntegerRecords(client, args, "resume", "bin", 1, 200);
	}

	public static void QueryString(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 5; i++)
		{
			ExampleFixtureSupport.AssertBin(client, args, "querykey" + i, "querybin", "queryvalue" + i);
		}
	}

	public static void QueryInteger(IAerospikeClient client, Arguments args)
	{
		for (int i = 14; i <= 18; i++)
		{
			ExampleFixtureSupport.AssertBin(client, args, "querykeyint" + i, "querybinint", i);
		}
	}

	public static void QueryList(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "qlkey1", "listbin", new List<object> { "900", "902", "904" });
		ExampleFixtureSupport.AssertBin(client, args, "qlkey2", "listbin", new List<object> { "901", "905", "909" });
	}

	public static void QueryRegion(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBinExists(client, args, "querykeyloc0", "querybinloc");
		ExampleFixtureSupport.AssertBinExists(client, args, "querykeyloc19", "querybinloc");
	}

	public static void QueryRegionFilter(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBinExists(client, args, "filterkeyloc0", "filterloc");
	}

	public static void QueryFilter(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "profilekey2", "name", "Bill");
	}

	public static void QueryExp(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBinExists(client, args, 1, "idxbin");
	}

	public static void QueryPage(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, 1, "bin", 1, "pq");
	}

	public static void SetupQueryPage(IAerospikeClient client, Arguments args)
	{
		SetupQueryIntegerRecords(client, args, "pq", "pqidx", "bin", 1, 190);
	}

	public static void QueryResume(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, 1, "bin", 1, "qr");
	}

	public static void SetupQueryResume(IAerospikeClient client, Arguments args)
	{
		SetupQueryIntegerRecords(client, args, "qr", "qridx", "bin", 1, 200);
	}

	public static void QuerySum(IAerospikeClient client, Arguments args)
	{
		for (int i = 4; i <= 7; i++)
		{
			ExampleFixtureSupport.AssertBin(client, args, "aggkey" + i, "aggbin", i);
		}
	}

	public static void QueryAverage(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 10; i++)
		{
			ExampleFixtureSupport.AssertBin(client, args, "avgkey" + i, "l1", i);
			ExampleFixtureSupport.AssertBin(client, args, "avgkey" + i, "l2", 1);
		}
	}

	public static void QueryExecute(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "qekey3", "qebin1", 3L);
		ExampleFixtureSupport.AssertBin(client, args, "qekey4", "qebin1", 104L);
		ExampleFixtureSupport.AssertBin(client, args, "qekey5", "qebin1", 5L);
		AssertBinAbsent(client, args, "qekey5", "qebin2");
		ExampleFixtureSupport.AssertBin(client, args, "qekey6", "qebin1", 106L);
		ExampleFixtureSupport.AssertBin(client, args, "qekey8", "qebin1", 108L);
		ExampleFixtureSupport.AssertNotExists(client, args, "qekey9");
	}

	public static void QueryOpsProjection(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 5; i++)
		{
			ExampleFixtureSupport.AssertBin(client, args, "qopkey" + i, "test-bin-1", "value-1-" + i);
			ExampleFixtureSupport.AssertBin(client, args, "qopkey" + i, "test-bin-2", "value-2-" + i);
			ExampleFixtureSupport.AssertBin(client, args, "qopkey" + i, "counter", i * 50L);
			AssertMapValue(client, args, "qopkey" + i, "test-map-bin", "a", "map-val-" + i);
		}
	}

	public static void QueryGeoCollection(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBinExists(client, args, "map0", "geo_map_bin");
		ExampleFixtureSupport.AssertBinExists(client, args, "map999", "geo_map_bin");
		ExampleFixtureSupport.AssertBinExists(client, args, "list0", "geo_list_bin");
		ExampleFixtureSupport.AssertBinExists(client, args, "list999", "geo_list_bin");
	}

	public static void PathExpression(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBinExists(client, args, "pathexp_modify", "inventory");
		AssertMapValue(client, args, "pathexp_modify", "upd_inventory", ["inventory", "10000001", "variants", "2001", "quantity"], 110L);
		ExampleFixtureSupport.AssertBinExists(client, args, "pathexp_qs", "inventory");
		ExampleFixtureSupport.AssertBinExists(client, args, "pathexp_regex", "inventory");
		ExampleFixtureSupport.AssertBinExists(client, args, "pathexp_multi", "inventory");
		ExampleFixtureSupport.AssertBinExists(client, args, "pathexp_nofail", "inventory");
	}

	public static void PathExpressionEnhanced(IAerospikeClient client, Arguments args)
	{
		AssertMapValue(client, args, "pathexp1", "mapbin", "Charlie", 55L);
		AssertMapValue(client, args, "pathexp2", "mapbin", "Jim", 98L);
		ExampleFixtureSupport.AssertBin(client, args, "pathexp3", "color", "blue");
		ExampleFixtureSupport.AssertBin(client, args, "pathexp3", "size", 10L);
		AssertMapValue(client, args, "pathexp4", "mapbin", "John", 76L);
		AssertMapValue(client, args, "pathexp5", "mapbin", "Charlie", 55L);
	}

	public static void AsyncPutGet(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "putgetkey", "bin1", "value");
	}

	public static void AsyncBatch(IAerospikeClient client, Arguments args)
	{
		Batch(client, args);
	}

	public static void AsyncQuery(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertExists(client, args, "asqkey1");
	}

	public static void AsyncTransaction(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, 1, "a", "val1");
		ExampleFixtureSupport.AssertBin(client, args, 2, "b", "val2");
	}

	public static void AsyncScanPage(IAerospikeClient client, Arguments args)
	{
		int recordCount = 0;
		ScanPolicy policy = new();
		client.ScanAll(policy, args.ns, "apage", (Key key, Record record) => recordCount++);

		if (recordCount == 0)
		{
			throw new Exception("AsyncScanPage verification failed: no records scanned.");
		}
	}

	public static void SetupAsyncScanPage(IAerospikeClient client, Arguments args)
	{
		SetupIntegerRecords(client, args, "apage", "bin", 1, 200);
	}

	private static void AssertBinAbsent(IAerospikeClient client, Arguments args, object userKey, string binName)
	{
		Record record = client.Get(args.policy, ExampleFixtureSupport.Key(args, userKey), binName);

		if (record == null || record.GetValue(binName) != null)
		{
			throw new Exception($"Verification failed: expected {userKey}/{binName} to be absent.");
		}
	}

	private static void AssertMapValue(
		IAerospikeClient client,
		Arguments args,
		object userKey,
		string binName,
		object mapKey,
		object expected)
	{
		AssertMapValue(client, args, userKey, binName, [mapKey], expected);
	}

	private static void AssertMapValue(
		IAerospikeClient client,
		Arguments args,
		object userKey,
		string binName,
		IEnumerable<object> path,
		object expected)
	{
		Record record = client.Get(args.policy, ExampleFixtureSupport.Key(args, userKey), binName);
		object current = record?.GetValue(binName);

		foreach (object key in path)
		{
			if (current is not IDictionary map || !ExampleFixtureSupport.TryGetMapValue(map, key, out current))
			{
				throw new Exception($"Verification failed: expected {userKey}/{binName} path {string.Join("/", path)} to exist.");
			}
		}

		if (!ExampleFixtureSupport.CdtEquals(current, expected))
		{
			throw new Exception(
				$"Verification failed: expected {userKey}/{binName} path {string.Join("/", path)}=" +
				$"{ExampleValueFormatter.Format(expected)}, got {ExampleValueFormatter.Format(current)}.");
		}
	}

	private static void SetupIntegerRecords(
		IAerospikeClient client,
		Arguments args,
		string setName,
		string binName,
		int begin,
		int end)
	{
		for (int i = begin; i <= end; i++)
		{
			ExampleFixtureSupport.PutBinsInSet(client, args, setName, i, new Bin(binName, i));
		}
	}

	private static void SetupQueryIntegerRecords(
		IAerospikeClient client,
		Arguments args,
		string setName,
		string indexName,
		string binName,
		int begin,
		int end)
	{
		ExampleFixtureSupport.CreateIndex(client, args, setName, indexName, binName, IndexType.INTEGER);
		SetupIntegerRecords(client, args, setName, binName, begin, end);
	}
}
