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

public sealed class QueryRegionFilter : SyncExample
{
	/// <summary>
	/// Run a geospatial query and apply a server-side Lua filter to the results.
	/// </summary>
	public override void RunExample()
	{
		const string binName = "filterloc";
		const string amenity = "school";

		string region = """
			{ "type": "Polygon", "coordinates": [
				[[-122.500000, 37.000000], [-121.000000, 37.000000],
				 [-121.000000, 38.080000], [-122.500000, 38.080000],
				 [-122.500000, 37.000000]]
			] }
			""";

		console.Info($"QueryRegion: {region}");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.GeoWithinRegion(binName, region)
		};
		stmt.SetAggregateFunction("geo_filter_example", "match_amenity", Value.Get(amenity));

		using ResultSet rs = client.QueryAggregate(null, stmt);

		while (rs.Next())
		{
			console.Info($"Record found: {rs.Object}");
		}
	}
}
