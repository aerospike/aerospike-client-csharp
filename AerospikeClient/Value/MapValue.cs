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
using System.Collections.Generic;

namespace Aerospike.Client
{
	partial class Value
	{
		/// <summary>
		/// Map value.
		/// </summary>
		public sealed class MapValue : Value, IEquatable<MapValue>, IEquatable<IDictionary>
		{
			public IDictionary Map { get; }
			public MapOrder Order { get; }
			public byte[] Bytes { get; private set; }

			public override int Type { get => ParticleType.MAP; }

			public override object Object { get => Map; }

			public MapValue(IDictionary map)
			{
				Map = map;
				Order = MapOrder.UNORDERED;
				Bytes = default;
			}

			public MapValue(IDictionary map, MapOrder order)
			{
				Map = map;
				Order = order;
				Bytes = default;
			}

			public override int EstimateSize()
			{
				Bytes = Packer.Pack(Map, Order);
				return Bytes.Length;
			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
				return Bytes.Length;
			}

			public override void Pack(Packer packer) => packer.PackMap(Map, Order);

			public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: map");

			public override string ToString() => Map.ToString();

			public override bool Equals(object obj)
			{
				if (obj is IDictionary dValue) return Equals(dValue);
				if (obj is MapValue mValue) return Equals(mValue);

				return false;
			}

			public bool Equals(MapValue other) => other is null || other.Map is null ? false : Map.Equals(other.Map);

			public bool Equals(IDictionary other)
			{
				if (other is null) return false;

				if (Map.Count != other.Count) return false;

				try
				{
					foreach (DictionaryEntry entry in Map)
					{
						object v1 = entry.Value;
						object v2 = other[entry.Key];

						if (v1 == null)
						{
							if (v2 == null) continue;
							return false;
						}

						if (!v1.Equals(v2)) return false;
					}
					return true;
				}
				catch (Exception)
				{
					return false;
				}
			}

			public override int GetHashCode()
			{
				int result = 1;
				foreach (DictionaryEntry entry in Map)
				{
					result = 31 * result + (entry.Key == null ? 0 : entry.Key.GetHashCode());
					result = 31 * result + (entry.Value == null ? 0 : entry.Value.GetHashCode());
				}
				return result;
			}

			public static bool operator ==(MapValue o1, MapValue o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(MapValue o1, MapValue o2) => o1 == o2 ? false : true;

			public static bool operator ==(MapValue o1, IDictionary o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(MapValue o1, IDictionary o2) => o1 == o2 ? false : true;
		}
	}
}
