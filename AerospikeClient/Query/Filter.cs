/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter Equal(string name, long value, params CTX[] ctx)
		{
			Value val = Value.Get(value);
			return new Filter(name, IndexCollectionType.DEFAULT, val.Type, val, val, ctx);
		}

		/// <summary>
		/// Create long equality filter for query by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter Equal(Expression exp, long value)
		{
			Value val = Value.Get(value);
			return new Filter(null, exp, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create long equality filter for query by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter EqualByIndex(string indexName, long value)
		{
			Value val = Value.Get(value);
			return new Filter(indexName, null, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create string equality filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="value">filter value</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter Equal(string name, string value, params CTX[] ctx)
		{
			Value val = Value.Get(value);
			return new Filter(name, IndexCollectionType.DEFAULT, val.Type, val, val, ctx);
		}

		/// <summary>
		/// Create string equality filter for query by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter Equal(Expression exp, string value)
		{
			Value val = Value.Get(value);
			return new Filter(null, exp, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create string equality filter for query by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter EqualByIndex(string indexName, string value)
		{
			Value val = Value.Get(value);
			return new Filter(indexName, null, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create blob equality filter for query.
		/// Requires server version 7.0+
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="value">filter value</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		/// <returns>filter instance</returns>
		public static Filter Equal(string name, byte[] value, params CTX[] ctx)
		{
			Value val = Value.Get(value);
			return new Filter(name, IndexCollectionType.DEFAULT, val.Type, val, val, ctx);
		}


		/// <summary>
		/// Create blob equality filter for query by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter Equal(Expression exp, byte[] value)
		{
			Value val = Value.Get(value);
			return new Filter(null, exp, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create blob equality filter for query by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter EqualByIndex(string indexName, byte[] value)
		{
			Value val = Value.Get(value);
			return new Filter(indexName, null, IndexCollectionType.DEFAULT, val.Type, val, val);
		}

		/// <summary>
		/// Create contains number filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter Contains(string name, IndexCollectionType type, long value, params CTX[] ctx)
		{
			Value val = Value.Get(value);
			return new Filter(name, type, val.Type, val, val, ctx);
		}

		/// <summary>
		/// Create contains number filter for query on collection index by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		public static Filter Contains(Expression exp, IndexCollectionType type, long value)
		{
			Value val = Value.Get(value);
			return new Filter(null, exp, type, val.Type, val, val);
		}

		/// <summary>
		/// Create contains number filter for query on collection index by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		public static Filter ContainsByIndex(string indexName, IndexCollectionType type, long value)
		{
			Value val = Value.Get(value);
			return new Filter(indexName, null, type, val.Type, val, val);
		}

		/// <summary>
		/// Create contains string filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter Contains(string name, IndexCollectionType type, string value, params CTX[] ctx)
		{
			Value val = Value.Get(value);
			return new Filter(name, type, val.Type, val, val, ctx);
		}

		/// <summary>
		/// Create contains string filter for query on collection index by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		public static Filter Contains(Expression exp, IndexCollectionType type, string value)
		{
			Value val = Value.Get(value);
			return new Filter(null, exp, type, val.Type, val, val);
		}

		/// <summary>
		/// Create contains string filter for query on collection index by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		public static Filter ContainsByIndex(string indexName, IndexCollectionType type, string value)
		{
			Value val = Value.Get(value);
			return new Filter(indexName, null, type, val.Type, val, val);
		}

		/// <summary>
		/// Create contains byte[] filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		/// <returns>filter instance</returns>
		public static Filter Contains(string name, IndexCollectionType type, byte[] value, params CTX[] ctx)
		{
			Value val = Value.Get(value);
			return new Filter(name, type, val.Type, val, val, ctx);
		}

		/// <summary>
		/// Create contains byte[] filter for query on collection index by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter Contains(Expression exp, IndexCollectionType type, byte[] value)
		{
			Value val = Value.Get(value);
			return new Filter(null, exp, type, val.Type, val, val);
		}

		/// <summary>
		/// Create contains byte[] filter for query on collection index by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="type">index collection type</param>
		/// <param name="value">filter value</param>
		/// <returns>filter instance</returns>
		public static Filter ContainsByIndex(string indexName, IndexCollectionType type, byte[] value)
		{
			Value val = Value.Get(value);
			return new Filter(indexName, null, type, val.Type, val, val);
		}

		/// <summary>
		/// Create range filter for query.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="begin">filter begin value inclusive</param>
		/// <param name="end">filter end value inclusive</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter Range(string name, long begin, long end, params CTX[] ctx)
		{
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.INTEGER, Value.Get(begin), Value.Get(end), ctx);
		}

		/// <summary>
		/// Create range filter for query by expression.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="begin">filter begin value inclusive</param>
		/// <param name="end">filter end value inclusive</param>
		public static Filter Range(Expression exp, long begin, long end)
		{
			return new Filter(null, exp, IndexCollectionType.DEFAULT, ParticleType.INTEGER, Value.Get(begin), Value.Get(end));
		}

		/// <summary>
		/// Create range filter for query by index name.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="begin">filter begin value inclusive</param>
		/// <param name="end">filter end value inclusive</param>
		public static Filter RangeByIndex(string indexName, long begin, long end)
		{
			return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.INTEGER, Value.Get(begin), Value.Get(end));
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
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter Range(string name, IndexCollectionType type, long begin, long end, params CTX[] ctx)
		{
			return new Filter(name, type, ParticleType.INTEGER, Value.Get(begin), Value.Get(end), ctx);
		}

		/// <summary>
		/// Create range filter for query on collection index by expression.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="type">index collection type inclusive</param>
		/// <param name="begin">filter begin value inclusive</param>
		/// <param name="end">filter end value inclusive</param>
		public static Filter Range(Expression exp, IndexCollectionType type, long begin, long end)
		{
			return new Filter(null, exp, type, ParticleType.INTEGER, Value.Get(begin), Value.Get(end));
		}

		/// <summary>
		/// Create range filter for query on collection index by index name.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="type">index collection type inclusive</param>
		/// <param name="begin">filter begin value inclusive</param>
		/// <param name="end">filter end value inclusive</param>
		public static Filter RangeByIndex(string indexName, IndexCollectionType type, long begin, long end)
		{
			return new Filter(indexName, null, type, ParticleType.INTEGER, Value.Get(begin), Value.Get(end));
		}

		/// <summary>
		/// Create geospatial "within region" filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="region">GeoJSON region</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter GeoWithinRegion(string name, string region, params CTX[] ctx)
		{
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(region), Value.Get(region), ctx);
		}

		/// <summary>
		/// Create geospatial "within region" filter for query by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="region">GeoJSON region</param>
		public static Filter GeoWithinRegion(Expression exp, string region)
		{
			return new Filter(null, exp, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(region), Value.Get(region));
		}

		/// <summary>
		/// Create geospatial "within region" filter for query by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="region">GeoJSON region</param>
		public static Filter GeoWithinRegionByIndex(string indexName, string region)
		{
			return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(region), Value.Get(region));
		}

		/// <summary>
		/// Create geospatial "within region" filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="region">GeoJSON region</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter GeoWithinRegion(string name, IndexCollectionType type, string region, params CTX[] ctx)
		{
			return new Filter(name, type, ParticleType.GEOJSON, Value.Get(region), Value.Get(region), ctx);
		}

		/// <summary>
		/// Create geospatial "within region" filter for query on collection index by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="type">index collection type</param>
		/// <param name="region">GeoJSON region</param>
		public static Filter GeoWithinRegion(Expression exp, IndexCollectionType type, string region)
		{
			return new Filter(null, exp, type, ParticleType.GEOJSON, Value.Get(region), Value.Get(region));
		}

		/// <summary>
		/// Create geospatial "within region" filter for query on collection index by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="type">index collection type</param>
		/// <param name="region">GeoJSON region</param>
		public static Filter GeoWithinRegionByIndex(string indexName, IndexCollectionType type, string region)
		{
			return new Filter(indexName, null, type, ParticleType.GEOJSON, Value.Get(region), Value.Get(region));
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter GeoWithinRadius(string name, double lng, double lat, double radius, params CTX[] ctx)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr), ctx);
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		public static Filter GeoWithinRadius(Expression exp, double lng, double lat, double radius)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(null, exp, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr));
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		public static Filter GeoWithinRadiusByIndex(string indexName, double lng, double lat, double radius)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr));
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter GeoWithinRadius(string name, IndexCollectionType type, double lng, double lat, double radius, params CTX[] ctx)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(name, type, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr), ctx);
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query on collection index by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="type">index collection type</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		public static Filter GeoWithinRadius(Expression exp, IndexCollectionType type, double lng, double lat, double radius)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(null, exp, type, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr));
		}

		/// <summary>
		/// Create geospatial "within radius" filter for query on collection index by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="type">index collection type</param>
		/// <param name="lng">longitude</param>
		/// <param name="lat">latitude</param>
		/// <param name="radius">radius (meters)</param>
		public static Filter GeoWithinRadiusByIndex(string indexName, IndexCollectionType type, double lng, double lat, double radius)
		{
			string rgnstr = string.Format("{{ \"type\": \"AeroCircle\", " + "\"coordinates\": [[{0:F8}, {1:F8}], {2:F}] }}", lng, lat, radius);
			return new Filter(indexName, null, type, ParticleType.GEOJSON, Value.Get(rgnstr), Value.Get(rgnstr));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="point">GeoJSON point</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter GeoContains(string name, string point, params CTX[] ctx)
		{
			return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(point), Value.Get(point), ctx);
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="point">GeoJSON point</param>
		public static Filter GeoContains(Expression exp, string point)
		{
			return new Filter(null, exp, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="point">GeoJSON point</param>
		public static Filter GeoContainsByIndex(string indexName, string point)
		{
			return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query on collection index.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="type">index collection type</param>
		/// <param name="point">GeoJSON point</param>
		/// <param name="ctx">optional context for elements within a CDT</param>
		public static Filter GeoContains(string name, IndexCollectionType type, string point, params CTX[] ctx)
		{
			return new Filter(name, type, ParticleType.GEOJSON, Value.Get(point), Value.Get(point), ctx);
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query on collection index by expression.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="exp">expression to be indexed</param>
		/// <param name="type">index collection type</param>
		/// <param name="point">GeoJSON point</param>
		public static Filter GeoContains(Expression exp, IndexCollectionType type, string point)
		{
			return new Filter(null, exp, type, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		/// <summary>
		/// Create geospatial "containing point" filter for query on collection index by index name.
		/// Requires server version 8.1+
		/// </summary>
		/// <param name="indexName">index name</param>
		/// <param name="type">index collection type</param>
		/// <param name="point">GeoJSON point</param>
		public static Filter GeoContainsByIndex(string indexName, IndexCollectionType type, string point)
		{
			return new Filter(indexName, null, type, ParticleType.GEOJSON, Value.Get(point), Value.Get(point));
		}

		private readonly string binName;
		private readonly IndexCollectionType colType;
		private readonly byte[] packedCtx;
		private readonly ParticleType valType;
		private readonly Value begin;
		private readonly Value end;
		public string IndexName { get; private set; }
		public Expression Exp { get; private set; }

		Filter(string binName, IndexCollectionType colType, int valType, Value begin, Value end, byte[] packedCtx, string indexName, Expression exp)
		{
			this.binName = binName;
			this.colType = colType;
			this.valType = (ParticleType)valType;
			this.begin = begin;
			this.end = end;
			this.packedCtx = packedCtx;
			this.IndexName = indexName;
			this.Exp = exp;
		}

		private Filter(string binName, IndexCollectionType colType, ParticleType valType, Value begin, Value end, CTX[] ctx) :
			this(binName, colType, (int)valType, begin, end, (ctx != null && ctx.Length > 0) ? PackUtil.Pack(ctx) : null, null, null)
		{ }

		private Filter(string indexName, Expression exp, IndexCollectionType colType, ParticleType valType, Value begin, Value end) :
			this(null, colType, (int)valType, begin, end, null, indexName, exp)
		{ }

		internal int EstimateSize()
		{
			// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
			return ByteUtil.EstimateSizeUtf8(binName) + begin.EstimateSize() + end.EstimateSize() + 10;
		}

		internal int Write(byte[] buf, int offset)
		{
			// Write name.
			int len = ByteUtil.StringToUtf8(binName, buf, offset + 1);
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

		public string Name
		{
			get { return binName; }
		}

		public IndexCollectionType ColType
		{
			get { return colType; }
		}

		public Value Begin
		{
			get { return begin; }
		}

		public Value End
		{
			get { return end; }
		}

		public int ValType
		{
			get { return (int)valType; }
		}

		internal IndexCollectionType CollectionType
		{
			get { return colType; }
		}

		internal byte[] PackedCtx
		{
			get { return packedCtx; }
		}
	}
}
