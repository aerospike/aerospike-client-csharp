/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryGeoCollection : SyncExample
	{
		public QueryGeoCollection(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Create secondary index on a string bin and query on it.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			RunMapExample(client, args);
			RunMapKeyExample(client, args);
			RunListExample(client, args);
		}

		private void RunMapExample(AerospikeClient client, Arguments args)
		{
			string indexName = "geo_map";
			string keyPrefix = "map";
			string mapValuePrefix = "mv";
			string binName = "geo_map_bin";
			string binName2 = "geo_uniq_bin";
			int size = 1000;

			// create collection index on mapValue
			CreateIndex(client, args, IndexCollectionType.MAPVALUES, indexName, binName);
			WriteMapRecords(client, args, keyPrefix, binName, binName2, mapValuePrefix, size);
			RunQuery(client, args, binName, binName2, IndexCollectionType.MAPVALUES);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
			DeleteRecords(client, args, keyPrefix, size);
		}

		private void RunMapKeyExample(AerospikeClient client, Arguments args)
		{
			string indexName = "geo_mapkey";
			string keyPrefix = "mapkey";
			string mapValuePrefix = "mk";
			string binName = "geo_mkey_bin";
			string binName2 = "geo_uniq_bin";
			int size = 1000;

			// create collection index on mapKey
			CreateIndex(client, args, IndexCollectionType.MAPKEYS, indexName, binName);
			WriteMapKeyRecords(client, args, keyPrefix, binName, binName2, mapValuePrefix, size);
			RunQuery(client, args, binName, binName2, IndexCollectionType.MAPKEYS);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
			DeleteRecords(client,args, keyPrefix, size);
		}

		private void RunListExample(AerospikeClient client, Arguments args)
		{
			string indexName = "geo_list";
			string keyPrefix = "list";
			string binName = "geo_list_bin";
			string binName2 = "geo_uniq_bin";
			int size = 1000;

			// create collection index on list
			CreateIndex(client, args, IndexCollectionType.LIST, indexName, binName);
			WriteListRecords(client, args, keyPrefix, binName, binName2, size);
			RunQuery(client, args, binName, binName2, IndexCollectionType.LIST);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
			DeleteRecords(client, args, keyPrefix, size);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, IndexCollectionType indexType, string indexName, string binName)
		{
			console.Info("Create GeoJSON {0} index: ns={1} set={2} index={3} bin={4}", indexType, args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.GEO2DSPHERE, indexType);
			task.Wait();
		}

		private void WriteMapRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, string binName2, string valuePrefix, int size)
		{
			for (int i = 0; i < size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Dictionary<string, Value> map = new Dictionary<string, Value>();

				for (int jj = 0; jj < 10; ++jj)
				{
					double plat = 0.0 + (0.01 * i);
					double plng = 0.0 + (0.10 * jj);
					string geoString = GeneratePoint(plat, plng);

					map[valuePrefix + "pointkey_" + i + "_" + jj] = Value.GetAsGeoJSON(geoString);

					double rlat = 0.0 + (0.01 * i);
					double rlng = 0.0 - (0.10 * jj);

					geoString = GeneratePolygon(rlat, rlng);

					map[valuePrefix + "regionkey_" + i + "_" + jj] = Value.GetAsGeoJSON(geoString);

				}
				Bin bin = new Bin(binName, map);
				Bin bin2 = new Bin(binName2, "other_bin_value_" + i);
				client.Put(args.writePolicy, key, bin, bin2);
			}

			console.Info("Write " + size + " records.");
		}

		private void WriteMapKeyRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, string binName2, string valuePrefix, int size)
		{
			for (int i = 0; i < size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Dictionary<Value, string> map = new Dictionary<Value, string>();

				for (int jj = 0; jj < 10; ++jj)
				{

					double plat = 0.0 + (0.01 * i);
					double plng = 0.0 + (0.10 * jj);
					string geoString = GeneratePoint(plat, plng);

					map[Value.GetAsGeoJSON(geoString)] = valuePrefix + "pointkey_" + i + "_" + jj;

					double rlat = 0.0 + (0.01 * i);
					double rlng = 0.0 - (0.10 * jj);

					geoString = GeneratePolygon(rlat, rlng);

					map[Value.GetAsGeoJSON(geoString)] = valuePrefix + "regionkey_" + i + "_" + jj;

				}
				Bin bin = new Bin(binName, map);
				Bin bin2 = new Bin(binName2, "other_bin_value_" + i);
				client.Put(args.writePolicy, key, bin, bin2);
			}

			console.Info("Write " + size + " records.");
		}

		private void WriteListRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, string binName2, int size)
		{
			for (int i = 0; i < size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				List<Value> mylist = new List<Value>();

				for (int jj = 0; jj < 10; ++jj)
				{

					double plat = 0.0 + (0.01 * i);
					double plng = 0.0 + (0.10 * jj);
					string geoString = GeneratePoint(plat, plng);

					mylist.Add(Value.GetAsGeoJSON(geoString));

					double rlat = 0.0 + (0.01 * i);
					double rlng = 0.0 - (0.10 * jj);

					geoString = GeneratePolygon(rlat, rlng);

					mylist.Add(Value.GetAsGeoJSON(geoString));

				}

				Bin bin = new Bin(binName, mylist);
				Bin bin2 = new Bin(binName2, "other_bin_value_" + i);
				client.Put(args.writePolicy, key, bin, bin2);
			}

			console.Info("Write " + size + " records.");
		}

		private void RunQuery(AerospikeClient client, Arguments args, string binName, string binName2, IndexCollectionType indexType)
		{
			console.Info("Query for: ns={0} set={1} bin={2} {3} within <region>", args.ns, args.set, binName, indexType.ToString());

			StringBuilder rgnsb = GenerateQueryRegion();

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.GeoWithinRegion(binName, indexType, rgnsb.ToString()));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;
				HashSet<string> uniques = new HashSet<string>();

				while (rs.Next())
				{
					Record record = rs.Record;
					string val = record.GetString(binName2);
					uniques.Add(val);
					count++;
				}

				if (count != 697)
				{
					console.Error("Query failed. {0} records expected. {1} returned.", 697, count);
				}
				else if (uniques.Count != 21)
				{
					console.Error("Query failed. {0} unique records expected. {1} unique returned.", 21, uniques.Count);
				}
				else
				{
					console.Info("query succeeded with {0} records {1} unique", count, uniques.Count);
				}
			}
			finally
			{
				rs.Close();
			}
		}

		private void DeleteRecords(AerospikeClient client, Arguments args, string keyPrefix, int size)
		{
			for (int i = 0; i < size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				client.Delete(args.writePolicy, key);
			}
		}

		private StringBuilder GenerateQueryRegion()
		{
			StringBuilder rgnsb = new StringBuilder();
			rgnsb.Append("{ ");
			rgnsb.Append("    \"type\": \"Polygon\", ");
			rgnsb.Append("    \"coordinates\": [[");
			rgnsb.Append("        [-0.202, -0.202], ");
			rgnsb.Append("        [ 0.202, -0.202], ");
			rgnsb.Append("        [ 0.202,  0.202], ");
			rgnsb.Append("        [-0.202,  0.202], ");
			rgnsb.Append("        [-0.202, -0.202] ");
			rgnsb.Append("    ]]");
			rgnsb.Append(" } ");
			return rgnsb;
		}

		private string GeneratePoint(double plat, double plng)
		{
			return string.Format("{{ \"type\": \"Point\", \"coordinates\": [{0:F6}, {1:F6}] }}", plng, plat);
		}

		private string GeneratePolygon(double rlat, double rlng)
		{
			return string.Format("{{ \"type\": \"Polygon\", \"coordinates\": [ [[{0:F6}, {1:F6}], [{2:F6}, {3:F6}], [{4:F6}, {5:F6}], [{6:F6}, {7:F6}], [{8:F6}, {9:F6}]] ] }}",
				rlng - 0.001, rlat - 0.001,
				rlng + 0.001, rlat - 0.001,
				rlng + 0.001, rlat + 0.001,
				rlng - 0.001, rlat + 0.001,
				rlng - 0.001, rlat - 0.001);
		}
	}
}
