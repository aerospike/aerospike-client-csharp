/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Container object for records.  Records are equivalent to rows.
	/// </summary>
	public sealed class Record
	{
		/// <summary>
		/// Map of requested name/value bins.
		/// </summary>
		public readonly Dictionary<string,object> bins;

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
		public Record(Dictionary<string,object> bins, int generation, int expiration)
		{
			this.bins = bins;
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

			object obj;
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
			return BitConverter.Int64BitsToDouble((long)GetValue(name));
		}

		/// <summary>
		/// Get bin value as float.
		/// </summary>
		public float GetFloat(string name)
		{
			return (float)BitConverter.Int64BitsToDouble((long)GetValue(name));
		}

		/// <summary>
		/// Get bin value as long.
		/// </summary>
		public long GetLong(string name)
		{
			return (long)GetValue(name);
		}

		/// <summary>
		/// Get bin value as ulong.
		/// </summary>
		public ulong GetULong(string name)
		{
			return (ulong)GetValue(name);
		}

		/// <summary>
		/// Get bin value as int.
		/// </summary>
		public int GetInt(string name)
		{
			return (int)(long)GetValue(name);
		}

		/// <summary>
		/// Get bin value as uint.
		/// </summary>
		public uint GetUInt(string name)
		{
			return (uint)(long)GetValue(name);
		}

		/// <summary>
		/// Get bin value as short.
		/// </summary>
		public short GetShort(string name)
		{
			return (short)(long)GetValue(name);
		}

		/// <summary>
		/// Get bin value as ushort.
		/// </summary>
		public ushort GetUShort(string name)
		{
			return (ushort)(long)GetValue(name);
		}

		/// <summary>
		/// Get bin value as byte.
		/// </summary>
		public byte GetByte(string name)
		{
			return (byte)(long)GetValue(name);
		}

		/// <summary>
		/// Get bin value as sbyte.
		/// </summary>
		public sbyte GetSBytes(string name)
		{
			return (sbyte)(long)GetValue(name);
		}

		/// <summary>
		/// Get bin value as bool.
		/// </summary>
		public bool GetBool(string name)
		{
			long v = (long)GetValue(name);
			return (v != 0) ? true : false;
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
