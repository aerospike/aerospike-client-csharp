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
using System.Collections;

namespace Aerospike.Client
{
    /// <summary>
    /// Column name/value pair. 
    /// </summary>
    public struct Bin
	{
		/// <summary>
		/// Bin name. Current limit is 15 characters.
		/// </summary>
		public string name { get; }

		/// <summary>
		/// Bin value.
		/// </summary>
		public Value value { get; }

		/// <summary>
		/// Constructor, specifying bin name and value.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, Value value)
		{
			this.name = name;
			this.value = value;
		}

		/// <summary>
		/// Constructor, specifying bin name and string value.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, string value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and byte array value.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, byte[] value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and byte array segment value.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">byte array value</param>
		/// <param name="offset">byte array segment offset</param>
		/// <param name="length">byte array segment length</param>
		public Bin(string name, byte[] value, int offset, int length)
		{
			this.name = name;
			this.value = Value.Get(value, offset, length);
		}

		/// <summary>
		/// Constructor, specifying bin name and double value.
		/// <para>
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </para>
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, double value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and float value.
		/// <para>
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </para>
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, float value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and long value.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, long value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and unsigned long value.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, ulong value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and integer value.
		/// The server will convert all integers to longs.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, int value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and unsigned integer value.
		/// The server will convert all integers to longs.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, uint value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and short value.
		/// The server will convert all shorts to longs.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, short value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and unsigned short value.
		/// The server will convert all shorts to longs.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, ushort value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and boolean value.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// Either a boolean or integer bin is sent to the server, depending
		/// on configuration <see cref="Value.UseBoolBin"/>.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, bool value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and byte value.
		/// The server will convert all byte integers to longs.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, byte value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Constructor, specifying bin name and signed byte value.
		/// The server will convert all byte integers to longs.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, sbyte value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Create bin with a list value.  The list value will be serialized as a server list type.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, IList value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Create bin with a map value.  The map value will be serialized as a server map type.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public Bin(string name, IDictionary value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

		/// <summary>
		/// Create bin with a map value and order.  The map value will be serialized as a server map type.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value, pass in TreeMap instance if map order is sorted.</param>
		/// <param name="mapOrder">map sorted order</param>
		public Bin(string name, IDictionary value, MapOrder mapOrder)
		{
			this.name = name;
			this.value = Value.Get(value, mapOrder);
		}

		/// <summary>
		/// Create bin with an object value. This is the slowest of the Bin constructors because
		/// the type must be determined using multiple "instanceof" checks. If the object type is
		/// unrecognized, BinaryFormatter is used to serialize the object.
		/// <para>
		/// To disable this constructor, set <see cref="Value.DisableSerializer"/> to true.
		/// </para>
		/// </summary>
		/// <param name="name">
		/// bin name, current limit is 15 characters. For servers configured as "single-bin", enter
		/// a null or empty name.
		/// </param>
		/// <param name="value">bin value</param>
		public Bin(string name, object value)
		{
			this.name = name;
			this.value = Value.Get(value);
		}

#if BINARY_FORMATTER
		/// <summary>
		/// Create bin with a blob value.  The value will be serialized by BinaryFormatter.
		/// This method is faster than the bin object constructor because the blob is converted 
		/// directly instead of using multiple "instanceof" type checks with a blob default.
		/// <para>
		/// To disable this method, set <see cref="Value.DisableSerializer"/> to true.
		/// </para>
		/// </summary>
		/// <param name="name">
		/// bin name, current limit is 15 characters. For servers configured as "single-bin", enter
		/// a null or empty name.
		/// </param>
		/// <param name="value">bin value</param>
		public static Bin AsBlob(string name, object value)
		{
			return new Bin(name, Value.GetAsBlob(value));
		}
#endif

		/// <summary>
		/// Create bin with a null value. This is useful for bin deletions within a record.
		/// For servers configured as "single-bin", enter a null or empty name.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		public static Bin AsNull(string name)
		{
			return new Bin(name, Value.AsNull);
		}

		/// <summary>
		/// Create bin with a GeoJSON value.
		/// </summary>
		/// <param name="name">bin name, current limit is 15 characters</param>
		/// <param name="value">bin value</param>
		public static Bin AsGeoJSON(string name, string value)
		{
			return new Bin(name, Value.GetAsGeoJSON(value));
		}
	
		/// <summary>
		/// Return string representation of bin.
		/// </summary>
		public override string ToString()
		{
			return name + ':' + value;
		}
	}
}
