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
using System.Text;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryRegion : SyncExample
	{
		public QueryRegion(Console console) : base(console)
		{
		}

		/// <summary>
		/// Geospatial query examples.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasGeo)
			{
				console.Info("Geospatial functions are not supported by the connected Aerospike server.");
				return;
			}
			
			string indexName = "queryindexloc";
			string keyPrefix = "querykeyloc";
			string binName = args.GetBinName("querybinloc");
			int size = 20;

			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName, size);
			RunQuery(client, args, indexName, binName);
			RunRadiusQuery(client, args, indexName, binName);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.GEO2DSPHERE);
			task.Wait();
		}

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
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
				Bin bin = Bin.AsGeoJSON(binName, ptsb.ToString());
				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName)
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

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.GeoWithinRegion(binName, rgnsb.ToString()));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Record record = rs.Record;
					string result = record.GetGeoJSON(binName);

					console.Info("Record found: " + result);
					count++;
				}

				if (count != 6)
				{
					console.Error("Query count mismatch. Expected 6. Received " + count);
				}
			}
			finally
			{
				rs.Close();
			}
		}

		private void RunRadiusQuery(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			double lon = -122.0;
			double lat = 37.5;
			double radius = 50000.0;
			console.Info("QueryRadius long=" + lon + " lat= " + lat + " radius=" + radius);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.GeoWithinRadius(binName, lon, lat, radius));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Record record = rs.Record;
					string result = record.GetGeoJSON(binName);

					console.Info("Record found: " + result);
					count++;
				}

				if (count != 4)
				{
					console.Error("Query count mismatch. Expected 4. Received " + count);
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
