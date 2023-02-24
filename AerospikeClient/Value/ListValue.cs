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
	partial class Value
	{
		/// <summary>
		/// List value.
		/// </summary>
		public sealed class ListValue : Value, IEquatable<ListValue>, IEquatable<IList>
		{
			public IList List { get; }
			public byte[] Bytes { get; private set; }

			public override int Type { get => ParticleType.LIST; }

			public override object Object { get => List; }

			public ListValue(IList list)
			{
				this.List = list;
				Bytes = default;
			}

			public override int EstimateSize()
			{
				Bytes = Packer.Pack(List);
				return Bytes.Length;
			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
				return Bytes.Length;
			}

			public override void Pack(Packer packer) => packer.PackList(List);

			public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: list");

			public override string ToString() => List.ToString();

			public override bool Equals(object obj)
			{
				if (obj is IList iValue) return Equals(iValue);
				if (obj is ListValue lValue) return Equals(lValue);

				return false;
			}

			public bool Equals(ListValue other) => other is null || other.List is null ? false : List.Equals(other.List);

			public bool Equals(IList other)
			{
				if (other is null) return false;

				if (List.Count != other.Count) return false;

				for (int i = 0; i < List.Count; i++)
				{
					object v1 = List[i];
					object v2 = other[i];

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
				foreach (object value in List)
				{
					result = 31 * result + (value == null ? 0 : value.GetHashCode());
				}
				return result;
			}

			public static bool operator ==(ListValue o1, ListValue o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(ListValue o1, ListValue o2) => o1 == o2 ? false : true;

			public static bool operator ==(ListValue o1, IList o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(ListValue o1, IList o2) => o1 == o2 ? false : true;
		}
	}
}
