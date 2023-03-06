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

namespace Aerospike.Client
{
	partial class Value
	{
		/// <summary>
		/// GeoJSON value.
		/// </summary>
		public sealed class GeoJSONValue : Value, IEquatable<GeoJSONValue>, IEquatable<string>
		{
			public string value { get; }

			public override ParticleType Type { get => ParticleType.GEOJSON; }

			public override object Object { get => value; }

			public GeoJSONValue(string value)
			{
				this.value = value;
			}

			// flags + ncells + jsonstr
			public override int EstimateSize() => 1 + 2 + ByteUtil.EstimateSizeUtf8(value);

			public override int Write(byte[] buffer, int offset)
			{
				buffer[offset] = 0; // flags
				ByteUtil.ShortToBytes(0, buffer, offset + 1); // ncells
				return 1 + 2 + ByteUtil.StringToUtf8(value, buffer, offset + 3); // jsonstr
			}

			public override void Pack(Packer packer) => packer.PackGeoJSON(value);

			public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: GeoJSON");

			public override string ToString() => value;

			public override bool Equals(object obj)
			{
				if (obj is string sValue) return Equals(sValue);
				if (obj is GeoJSONValue gValue) return Equals(gValue);

				return false;
			}

			public bool Equals(GeoJSONValue other) => other is null || other.value is null ? false : value.Equals(other.value);

			public bool Equals(string other) => other is null ? false : value.Equals(other);

			public override int GetHashCode() => value.GetHashCode();

			public static bool operator ==(GeoJSONValue o1, GeoJSONValue o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(GeoJSONValue o1, GeoJSONValue o2) => o1 == o2 ? false : true;

			public static bool operator ==(GeoJSONValue o1, string o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(GeoJSONValue o1, string o2) => o1 == o2 ? false : true;
		}
	}
}
