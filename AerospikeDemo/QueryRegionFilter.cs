/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System.Text;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryRegionFilter : SyncExample
	{
		public QueryRegionFilter(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Geospatial query with filter example.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			string indexName = "filterindexloc";
			string keyPrefix = "filterkeyloc";
			string binName1 = args.GetBinName("filterloc");
			string binName2 = args.GetBinName("filteramenity");
			int size = 20;

			Register(client, args);
			CreateIndex(client, args, indexName, binName1);
			WriteRecords(client, args, keyPrefix, binName1, binName2, size);
			RunQuery(client, args, indexName, binName1, binName2);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void Register(IAerospikeClient client, Arguments args)
		{
			string packageName = "geo_filter_example.lua";
			console.Info("Register: " + packageName);
			LuaExample.Register(client, args.policy, packageName);
		}
		
		private void CreateIndex(IAerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.GEO2DSPHERE);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}
		}

		private void WriteRecords(IAerospikeClient client, Arguments args, string keyPrefix, string binName1, string binName2, int size)
		{
			console.Info("Write " + size + " records.");

			for (int i = 0; i < size; i++)
			{
				double lng = -122 + (0.1 * i);
				double lat = 37.5 + (0.1 * i);
				StringBuilder ptsb = new StringBuilder();
				ptsb.Append("{ \"type\": \"Point\", \"coordinates\": [");
				ptsb.Append(lng);
				ptsb.Append(", ");
				ptsb.Append(lat);
				ptsb.Append("] }");
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin1 = Bin.AsGeoJSON(binName1, ptsb.ToString());
				Bin bin2;
				if (i % 7 == 0)
				{
					bin2 = new Bin(binName2, "hospital");
				}
				else if (i % 2 == 0)
				{
					bin2 = new Bin(binName2, "school");
				}
				else
				{
					bin2 = new Bin(binName2, "store");
				}
				client.Put(args.writePolicy, key, bin1, bin2);
			}
		}

		private void RunQuery(IAerospikeClient client, Arguments args, string indexName, string binName1, string binName2)
		{
			StringBuilder rgnsb = new StringBuilder();

			rgnsb.Append("{ ");
			rgnsb.Append("    \"type\": \"Polygon\", ");
			rgnsb.Append("    \"coordinates\": [ ");
			rgnsb.Append("        [[-122.500000, 37.000000],[-121.000000, 37.000000], ");
			rgnsb.Append("         [-121.000000, 38.080000],[-122.500000, 38.080000], ");
			rgnsb.Append("         [-122.500000, 37.000000]] ");
			rgnsb.Append("    ] ");
			rgnsb.Append(" } ");

			console.Info("QueryRegion: " + rgnsb);

			string amenStr = "school";

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.GeoWithinRegion(binName1, rgnsb.ToString()));
			stmt.SetAggregateFunction("geo_filter_example", "match_amenity", Value.Get(amenStr));

			ResultSet rs = client.QueryAggregate(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					object result = rs.Object;
					console.Info("Record found: " + result);
					count++;
				}

				if (count != 2)
				{
					console.Error("Wrong number of schools found. %d != 2", count);
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
