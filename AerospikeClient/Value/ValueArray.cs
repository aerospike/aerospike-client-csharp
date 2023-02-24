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
		/// Value array.
		/// </summary>
		public sealed class ValueArray : Value, IEquatable<ValueArray>, IEquatable<Value[]>
		{
			public Value[] Array { get; }
			public byte[] Bytes { get; set; }

			public override int Type { get => ParticleType.LIST; }

			public override object Object { get => Array; }

			public ValueArray(Value[] array)
			{
				Array = array;
			}

			public override int EstimateSize()
			{
				Bytes = Packer.Pack(Array);
				return Bytes.Length;
			}

			public override int Write(byte[] buffer, int offset)
			{
				System.Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
				return Bytes.Length;
			}

			public override void Pack(Packer packer) => packer.PackValueArray(Array);

			public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: value[]");

			public override string ToString() => Util.ArrayToString(Array);

			public override bool Equals(object obj)
			{
				if (obj is Value[] vValue) return Equals(vValue);
				if (obj is ValueArray vaValue) return Equals(vaValue);

				return false;
			}

			public bool Equals(ValueArray other) => other is null || other.Array is null ? false : Array.Equals(other.Array);

			public bool Equals(Value[] other)
			{
				if (other is null) return false;

				if (Array.Length != other.Length) return false;

				for (int i = 0; i < Array.Length; i++)
				{
					Value v1 = Array[i];
					Value v2 = other[i];

					if (v1 == null)
					{
						if (v2 == null) continue;
						return false;
					}

					if (!v1.Equals(v2)) return false;
				}
				return true;
			}

			public override int GetHashCode()
			{
				int result = 1;
				foreach (Value item in Array)
				{
					result = 31 * result + (item == null ? 0 : item.GetHashCode());
				}
				return result;
			}

			public static bool operator ==(ValueArray o1, ValueArray o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(ValueArray o1, ValueArray o2) => o1 == o2 ? false : true;

			public static bool operator ==(ValueArray o1, Value[] o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(ValueArray o1, Value[] o2) => o1 == o2 ? false : true;
		}
	}
}
