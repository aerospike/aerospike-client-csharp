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
namespace Aerospike.Client
{
	/// <summary>
	/// Query filter used to narrow down query results.
	/// </summary>
	public sealed class Filter
	{
		/// <summary>
		/// Create long equality filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="value">filter value</param>
		public static Filter Equal(string name, long value)
		{
			Value val = Value.Get(value);
			return new Filter(name, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create string equality filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="value">filter value</param>
		public static Filter Equal(string name, string value)
		{
			Value val = Value.Get(value);
			return new Filter(name, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create contains number filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		public static Filter Contains(string name, IndexCollectionType type, long value)
		{
			Value val = Value.Get(value);
			return new Filter(name, type, val.Type,  val, val);
		}

		/// <summary>
		/// Create contains string filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		public static Filter Contains(string name, IndexCollectionType type, string value)
		{
			Value val = Value.Get(value);
			return new Filter(name, type, val.Type, val, val);
		}
		
		/// <summary>
		/// Create range filter for query.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="begin">filter begin value inclusive</param>
		/// <param name="end">filter end value inclusive</param>
		public static Filter Range(string name, long begin, long end)
		{
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.INTEGER, Value.Get(begin), Value.Get(end));
		}

		/// <summary>
		/// Create range filter for query on collection index.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type inclusive</param>
		/// <param name="begin">filter begin value inclusive</param>
		/// <param name="end">filter end value</param>
		public static Filter Range(string name, IndexCollectionType type, long begin, long end)
		{
			return new Filter(name, type, ParticleType.INTEGER, Value.Get(begin), Value.Get(end));
		}

		/// <summary>
		/// Create geospatial "within region" filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="region">GeoJSON region</param>
		public static Filter GeoWithinRegion(string name, string region)
		{
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(region), Value.Get(region));
		}

		/// <summary>
		/// Create geospatial "within region" filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="region">GeoJSON region</param>
		public static Filter GeoWithinRegion(string name, IndexCollectionType type, string region)
		{
			return new Filter(name, type, ParticleType.GEOJSON, Value.Get(region), Value.Get(region));
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		public static Filter GeoWithinRadius(string name, double lng, double lat, double radius)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr));
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		public static Filter GeoWithinRadius(string name, IndexCollectionType type, double lng, double lat, double radius)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(name, type, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="point">GeoJSON point</param>
		public static Filter GeoContains(string name, string point)
		{
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="lon">longitude</param>
		/// <param name="lat">latitude</param>
		public static Filter GeoContains(string name, double lon, double lat)
		{
			string point = GeneratePoint(lat, lon);
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="point">GeoJSON point</param>
		public static Filter GeoContains(string name, IndexCollectionType type, string point)
		{
			return new Filter(name, type, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="lon">longitude</param>
		/// <param name="lat">latitude</param>
		public static Filter GeoContains(string name, IndexCollectionType type, double lon, double lat)
		{
			string point = GeneratePoint(lat, lon);
			return new Filter(name, type, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		private static string GeneratePoint(double plat, double plng)
		{
			return string.Format("{{ \"type\": \"Point\", \"coordinates\": [{0:F8}, {1:F8}] }}", plng, plat);
		}


		private readonly string name;
		private readonly IndexCollectionType colType;
		private readonly int valType;
		private readonly Value begin;
		private readonly Value end;

		private Filter(string name, IndexCollectionType colType, int valType, Value begin, Value end)
		{
			this.name = name;
			this.colType = colType;
			this.valType = valType;
			this.begin = begin;
			this.end = end;
		}

		internal int EstimateSize()
		{
			// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
			return ByteUtil.EstimateSizeUtf8(name) + begin.EstimateSize() + end.EstimateSize() + 10;
		}

		internal int Write(byte[] buf, int offset)
		{
			// Write name.
			int len = ByteUtil.StringToUtf8(name, buf, offset + 1);
			buf[offset] = (byte)len;
			offset += len + 1;

			// Write particle type.
			buf[offset++] = (byte)valType;

			// Write filter begin.
			len = begin.Write(buf, offset + 4);
			ByteUtil.IntToBytes((uint)len, buf, offset);
			offset += len + 4;

			// Write filter end.
			len = end.Write(buf, offset + 4);
			ByteUtil.IntToBytes((uint)len, buf, offset);
			offset += len + 4;

			return offset;
		}

		internal IndexCollectionType CollectionType
		{
			get {return colType;}
		}
	}
}
