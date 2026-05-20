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

public sealed class QueryRegion : SyncExample
{
	private const string IndexName = "queryindexloc";
	private const string BinName = "querybinloc";

	/// <summary>
	/// Geospatial query examples: GeoWithinRegion and GeoWithinRadius.
	/// </summary>
	public override void RunExample()
	{
		RunPolygonQuery();
		RunRadiusQuery();
	}

	private void RunPolygonQuery()
	{
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
			BinNames = [BinName],
			Filter = Filter.GeoWithinRegion(BinName, region)
		};

		using RecordSet rs = client.Query(null, stmt);

		while (rs.Next())
		{
			console.Info($"Record found: {rs.Record.GetGeoJSON(BinName)}");
		}
	}

	private void RunRadiusQuery()
	{
		const double lon = -122.0;
		const double lat = 37.5;
		const double radius = 50000.0;

		console.Info($"QueryRadius long={lon} lat={lat} radius={radius}");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			BinNames = [BinName],
			Filter = Filter.GeoWithinRadius(BinName, lon, lat, radius)
		};

		using RecordSet rs = client.Query(null, stmt);

		while (rs.Next())
		{
			console.Info($"Record found: {rs.Record.GetGeoJSON(BinName)}");
		}
	}
}
