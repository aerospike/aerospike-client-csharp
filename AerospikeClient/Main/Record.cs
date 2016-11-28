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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Container object for records.  Records are equivalent to rows.
	/// </summary>
	public sealed class Record
	{
		private static DateTime Epoch = new DateTime(2010, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

		/// <summary>
		/// Map of requested name/value bins.
		/// </summary>
		public readonly Dictionary<string,object> bins;

		/// <summary>
		/// Schema of Bins as a map of Particle types
		/// </summary>
		public readonly Dictionary<string, int> schema;

		/// <summary>
		/// Record modification count.
		/// </summary>
		public readonly int generation;

		/// <summary>
		/// Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
		/// </summary>
		public readonly int expiration;

		/// <summary>
		/// Initialize record.
		/// </summary>
		public Record(Dictionary<string,object> bins, Dictionary<string, int> schema, int generation, int expiration)
		{
			this.bins = bins;
			this.schema = schema;
			this.generation = generation;
			this.expiration = expiration;
		}

		/// <summary>
		/// Get bin value given bin name.
		/// Enter empty string ("") for servers configured as single-bin.
		/// </summary>
		public object GetValue(string name)
		{
			if (bins == null)
			{
				return null;
			}

			object obj = null;
			bins.TryGetValue(name, out obj);
			return obj;
		}

		/// <summary>
		/// Get bin value as string.
		/// </summary>
		public string GetString(string name)
		{
			return (string)GetValue(name);
		}

		/// <summary>
		/// Get bin value as double.
		/// </summary>
		public double GetDouble(string name)
		{
			// The server may return number as double or long.
			// Convert bits if returned as long.
			object result = GetValue(name);
			return (result is double) ? (double)result : (result != null) ? BitConverter.Int64BitsToDouble((long)result) : 0.0; 
		}

		/// <summary>
		/// Get bin value as float.
		/// </summary>
		public float GetFloat(string name)
		{
			return (float)GetDouble(name);
		}

		/// <summary>
		/// Get bin value as long.
		/// </summary>
		public long GetLong(string name)
		{
			// The server always returns numbers as longs if bin found.
			// If bin not found, the result will be null.  Convert null to zero.
			object result = GetValue(name);
			return (result != null) ? (long)result : 0;
		}

		/// <summary>
		/// Get bin value as ulong.
		/// </summary>
		public ulong GetULong(string name)
		{
			return (ulong)GetLong(name);
		}

		/// <summary>
		/// Get bin value as int.
		/// </summary>
		public int GetInt(string name)
		{
			// The server always returns numbers as longs, so get long and cast.
			return (int)GetLong(name);
		}

		/// <summary>
		/// Get bin value as uint.
		/// </summary>
		public uint GetUInt(string name)
		{
			// The server always returns numbers as longs, so get long and cast.
			return (uint)GetLong(name);
		}

		/// <summary>
		/// Get bin value as short.
		/// </summary>
		public short GetShort(string name)
		{
			// The server always returns numbers as longs, so get long and cast.
			return (short)GetLong(name);
		}

		/// <summary>
		/// Get bin value as ushort.
		/// </summary>
		public ushort GetUShort(string name)
		{
			// The server always returns numbers as longs, so get long and cast.
			return (ushort)GetLong(name);
		}

		/// <summary>
		/// Get bin value as byte.
		/// </summary>
		public byte GetByte(string name)
		{
			// The server always returns numbers as longs, so get long and cast.
			return (byte)GetLong(name);
		}

		/// <summary>
		/// Get bin value as sbyte.
		/// </summary>
		public sbyte GetSBytes(string name)
		{
			// The server always returns numbers as longs, so get long and cast.
			return (sbyte)GetLong(name);
		}

		/// <summary>
		/// Get bin value as bool.
		/// </summary>
		public bool GetBool(string name)
		{
			// The server always returns booleans as longs, so get long and convert.
			return (GetLong(name) != 0) ? true : false;
		}

		/// <summary>
		/// Get bin value as list.
		/// </summary>
		public IList GetList(string name)
		{
			return (IList)GetValue(name);
		}

		/// <summary>
		/// Get bin value as dictionary map.
		/// </summary>
		public IDictionary GetMap(string name)
		{
			return (IDictionary)GetValue(name);
		}

		/// <summary>
		/// Get bin value as GeoJSON.
		/// </summary>
		public string GetGeoJSON(string name)
		{
			return (string)GetValue(name);
		}
	
		/**
		 * Convert record expiration (seconds from Jan 01 2010 00:00:00 GMT) to
		 * ttl (seconds from now).
		 */
		public int TimeToLive
		{
			get
			{
				// This is the server's flag indicating the record never expires.
				if (expiration == 0)
				{
					// Convert to client-side convention for "never expires".
					return -1;
				}

				// Subtract epoch from current time.
				int now = (int)DateTime.UtcNow.Subtract(Epoch).TotalSeconds;

				// Record may not have expired on server, but delay or clock differences may
				// cause it to look expired on client. Floor at 1, not 0, to avoid old
				// "never expires" interpretation.
				return (expiration < 0 || expiration > now) ? expiration - now : 1;
			}
		}

		/// <summary>
		/// Return string representation of record.
		/// </summary>
		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(500);
			sb.Append("(gen:");
			sb.Append(generation);
			sb.Append("),(exp:");
			sb.Append(expiration);
			sb.Append("),(bins:");

			if (bins != null)
			{
				bool sep = false;

				foreach (KeyValuePair<string, object> entry in bins)
				{
					if (sep)
					{
						sb.Append(',');
					}
					else
					{
						sep = true;
					}
					sb.Append('(');
					sb.Append(entry.Key);
					sb.Append(':');
					sb.Append(entry.Value);
					sb.Append(')');

					if (sb.Length > 1000)
					{
						sb.Append("...");
						break;
					}
				}
			}
			else
			{
				sb.Append("null");
			}
			sb.Append(')');
			return sb.ToString();
		}
	}
}
