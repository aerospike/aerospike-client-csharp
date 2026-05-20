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

namespace Aerospike.Example;

internal static class QueryExampleFixtures
{
	public static void SetupQueryInteger(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "queryindexint", "querybinint", IndexType.INTEGER);
		PutIntegerRecords(client, args, "querykeyint", "querybinint", 1, 50);
	}

	public static void SetupQueryString(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "queryindex", "querybin", IndexType.STRING);

		for (int i = 1; i <= 5; i++)
		{
			ExampleFixtureSupport.PutBins(client, args, "querykey" + i, new Bin("querybin", "queryvalue" + i));
		}
	}

	public static void SetupQueryList(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "qlindex", "listbin", IndexType.STRING, IndexCollectionType.LIST);

		for (int i = 1; i <= 20; i++)
		{
			List<string> list = i % 2 == 0
				? ["901", "905", "909"]
				: ["900", "902", "904"];
			ExampleFixtureSupport.PutBins(client, args, "qlkey" + i, new Bin("listbin", list));
		}
	}

	public static void SetupQueryRegion(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "queryindexloc", "querybinloc", IndexType.GEO2DSPHERE);
		PutGeoRecords(client, args, "querykeyloc", "querybinloc", 20);
	}

	public static void SetupQueryRegionFilter(IAerospikeClient client, Arguments args)
	{
		LuaExample.Register(client, args.policy, "geo_filter_example.lua");
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "filterindexloc", "filterloc", IndexType.GEO2DSPHERE);

		for (int i = 0; i < 20; i++)
		{
			string amenity = i % 7 == 0
				? "hospital"
				: i % 2 == 0
					? "school"
					: "store";
			ExampleFixtureSupport.PutBins(
				client,
				args,
				"filterkeyloc" + i,
				Bin.AsGeoJSON("filterloc", Point(-122 + (0.1 * i), 37.5 + (0.1 * i))),
				new Bin("filteramenity", amenity));
		}
	}

	public static void SetupQueryFilter(IAerospikeClient client, Arguments args)
	{
		LuaExample.Register(client, args.policy, "filter_example.lua");
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "profileindex", "name", IndexType.STRING);
		ExampleFixtureSupport.PutBins(client, args, "profilekey1", new Bin("name", "Charlie"), new Bin("password", "cpass"));
		ExampleFixtureSupport.PutBins(client, args, "profilekey2", new Bin("name", "Bill"), new Bin("password", "hknfpkj"));
		ExampleFixtureSupport.PutBins(client, args, "profilekey3", new Bin("name", "Doug"), new Bin("password", "dj6554"));
	}

	public static void SetupQueryExp(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "predidx", "idxbin", IndexType.INTEGER);

		for (int i = 1; i <= 50; i++)
		{
			Bin bin3 = i % 4 == 0
				? new Bin("bin3", "prefix-" + i + "-suffix")
				: i % 2 == 0
					? new Bin("bin3", "prefix-" + i + "-SUFFIX")
					: new Bin("bin3", "pre-" + i + "-suf");
			ExampleFixtureSupport.PutBins(client, args, i, new Bin("idxbin", i), new Bin("bin2", i * 10), bin3);
		}
	}

	public static void SetupQuerySum(IAerospikeClient client, Arguments args)
	{
		const string packageContents = @"
local function reducer(val1,val2)
	return val1 + val2
end

function sum_single_bin(stream,name)
	local function mapper(rec)
		return rec[name]
	end
	return stream : map(mapper) : reduce(reducer)
end
";
		ExampleFixtureSupport.RegisterUdfString(client, packageContents, "sum_example.lua");
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "aggindex", "aggbin", IndexType.INTEGER);
		PutIntegerRecords(client, args, "aggkey", "aggbin", 1, 10);
	}

	public static void SetupQueryAverage(IAerospikeClient client, Arguments args)
	{
		LuaExample.Register(client, args.policy, "average_example.lua");
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "avgindex", "l2", IndexType.INTEGER);

		for (int i = 1; i <= 10; i++)
		{
			ExampleFixtureSupport.PutBins(client, args, "avgkey" + i, new Bin("l1", i), new Bin("l2", 1));
		}
	}

	public static void SetupQueryExecute(IAerospikeClient client, Arguments args)
	{
		LuaExample.Register(client, args.policy, "record_example.lua");
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "qeindex1", "qebin1", IndexType.INTEGER);

		for (int i = 1; i <= 10; i++)
		{
			ExampleFixtureSupport.PutBins(client, args, "qekey" + i, new Bin("qebin1", i), new Bin("qebin2", i));
		}
	}

	public static void SetupAsyncQuery(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "asqindex", "asqbin", IndexType.INTEGER);
		PutIntegerRecords(client, args, "asqkey", "asqbin", 1, 50);
	}

	public static void SetupQueryOpsProjection(IAerospikeClient client, Arguments args)
	{
		for (int i = 1; i <= 5; i++)
		{
			var mapData = new Dictionary<string, string>
			{
				{ "a", "map-val-" + i },
				{ "b", "other-" + i }
			};

			ExampleFixtureSupport.PutBins(
				client,
				args,
				"qopkey" + i,
				new Bin("test-bin-1", "value-1-" + i),
				new Bin("test-bin-2", "value-2-" + i),
				new Bin("test-map-bin", mapData),
				new Bin("counter", i * 50));
		}
	}

	public static void SetupQueryGeoCollection(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "geo_map", "geo_map_bin", IndexType.GEO2DSPHERE, IndexCollectionType.MAPVALUES);
		ExampleFixtureSupport.CreateIndex(client, args, args.set, "geo_list", "geo_list_bin", IndexType.GEO2DSPHERE, IndexCollectionType.LIST);

		for (int i = 0; i < 1000; i++)
		{
			Dictionary<string, Value> map = [];
			List<Value> list = [];

			for (int j = 0; j < 10; j++)
			{
				double lat = 0.01 * i;
				double lng = 0.10 * j;
				string point = Point(lng, lat);
				string polygon = Polygon(-lng, lat);

				map[$"mvpointkey_{i}_{j}"] = Value.GetAsGeoJSON(point);
				map[$"mvregionkey_{i}_{j}"] = Value.GetAsGeoJSON(polygon);
				list.Add(Value.GetAsGeoJSON(point));
				list.Add(Value.GetAsGeoJSON(polygon));
			}

			ExampleFixtureSupport.PutBins(
				client,
				args,
				$"map{i}",
				new Bin("geo_map_bin", map),
				new Bin("geo_uniq_bin", $"other_bin_value_{i}"));
			ExampleFixtureSupport.PutBins(
				client,
				args,
				$"list{i}",
				new Bin("geo_list_bin", list),
				new Bin("geo_uniq_bin", $"other_bin_value_{i}"));
		}
	}

	private static void PutIntegerRecords(IAerospikeClient client, Arguments args, string keyPrefix, string binName, int begin, int end)
	{
		for (int i = begin; i <= end; i++)
		{
			ExampleFixtureSupport.PutBins(client, args, keyPrefix + i, new Bin(binName, i));
		}
	}

	private static void PutGeoRecords(IAerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
	{
		for (int i = 0; i < size; i++)
		{
			ExampleFixtureSupport.PutBins(client, args, keyPrefix + i, Bin.AsGeoJSON(binName, Point(-122 + (0.1 * i), 37.5 + (0.1 * i))));
		}
	}

	private static string Point(double lng, double lat)
		=> string.Create(CultureInfo.InvariantCulture, $"{{ \"type\": \"Point\", \"coordinates\": [{lng:F6}, {lat:F6}] }}");

	private static string Polygon(double lng, double lat)
		=> string.Create(
			CultureInfo.InvariantCulture,
			$"{{ \"type\": \"Polygon\", \"coordinates\": [[" +
			$"[{lng - 0.001:F6}, {lat - 0.001:F6}], " +
			$"[{lng + 0.001:F6}, {lat - 0.001:F6}], " +
			$"[{lng + 0.001:F6}, {lat + 0.001:F6}], " +
			$"[{lng - 0.001:F6}, {lat + 0.001:F6}], " +
			$"[{lng - 0.001:F6}, {lat - 0.001:F6}]]] }}");
}
