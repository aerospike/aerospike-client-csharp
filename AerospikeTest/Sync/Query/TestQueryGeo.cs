/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryGeo : TestSync
	{
		private const string setName = "geo";
		private const string setNamePoints = "geopt";
		private const string setNameRegions = "georeg";
		private const string indexName = "geoidx";
		private const string binName = "geobin";
		private const int size = 20;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				socketTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask task = client.CreateIndex(policy, SuiteHelpers.ns, setName, indexName, binName, IndexType.GEO2DSPHERE);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			// Insert points
			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, setNamePoints, i);
				double lng = -122 + (0.1 * i);
				double lat = 37.5 + (0.1 * i);
				string loc = "{ \"type\": \"Point\", \"coordinates\": [" + lng + ", " + lat + "] }";
				Bin bin = Bin.AsGeoJSON("loc", loc);

				client.Put(null, key, bin);
			}

			// Insert regions
			double[][] starbucks = [[-122.1708441, 37.4241193], [-122.1492040, 37.4273569], [-122.1441078, 37.4268202], [-122.1251714, 37.4130590], [-122.0964289, 37.4218102], [-122.0776641, 37.4158199], [-122.0943475, 37.4114654], [-122.1122861, 37.4028493], [-122.0947230, 37.3909250], [-122.0831037, 37.3876090], [-122.0707119, 37.3787855], [-122.0303178, 37.3882739], [-122.0464861, 37.3786236], [-122.0582128, 37.3726980], [-122.0365083, 37.3676930]];

			for (int i = 0; i < starbucks.Length; i++)
			{
				Key key = new(SuiteHelpers.ns, setNameRegions, i);
				string loc = "{ \"type\": \"AeroCircle\", \"coordinates\": [[" + starbucks[i][0] + ", " + starbucks[i][1] + "], 3000.0 ] }";
				Bin bin = Bin.AsGeoJSON("loc", loc);

				client.Put(null, key, bin);
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, setName, indexName);
		}

		[TestMethod]
		public void QueryGeo1()
		{
			string region = "{ \"type\": \"Point\", \"coordinates\": [ -122.0986857, 37.4214209 ] }";

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setNameRegions);

			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.GeoCompare(Exp.GeoBin("loc"), Exp.Geo(region)))
			};

			RecordSet rs = client.Query(policy, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					//System.out.println(rs.getRecord().toString());
					count++;
				}
				Assert.AreEqual(5, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
