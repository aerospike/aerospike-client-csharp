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

public sealed class QueryGeoCollection : SyncExample
{
	private const string UniqueBinName = "geo_uniq_bin";

	private const string Region = """
		{ "type": "Polygon", "coordinates": [[
			[-0.202, -0.202],
			[ 0.202, -0.202],
			[ 0.202,  0.202],
			[-0.202,  0.202],
			[-0.202, -0.202]
		]] }
		""";

	/// <summary>
	/// Query a geospatial index attached to a map collection and to a list collection.
	/// </summary>
	public override void RunExample()
	{
		RunQuery("geo_map_bin", IndexCollectionType.MAPVALUES);
		RunQuery("geo_list_bin", IndexCollectionType.LIST);
	}

	private void RunQuery(string binName, IndexCollectionType indexType)
	{
		console.Info($"Query for: ns={ns} set={set} bin={binName} {indexType} within <region>");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.GeoWithinRegion(binName, indexType, Region)
		};

		using RecordSet rs = client.Query(null, stmt);

		HashSet<string> uniques = [];

		while (rs.Next())
		{
			uniques.Add(rs.Record.GetString(UniqueBinName));
		}

		console.Info($"Query returned {uniques.Count} unique records");
	}
}
