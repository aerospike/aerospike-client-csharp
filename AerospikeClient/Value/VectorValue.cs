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

namespace Aerospike.Client
{
	partial class Value
	{
		/// <summary>
		/// Vector value.
		/// </summary>
		public sealed class VectorValue : Value, IEquatable<VectorValue>, IEquatable<Vector>
		{
			public Vector Vector { get; }

			public override ParticleType Type => ParticleType.VECTOR;

			public override object Object { get => Vector; }

			public VectorValue(Vector vector)
			{
				this.Vector = vector;
			}

			public override int EstimateSize() => Vector.GetWireSize();

			public override int Write(byte[] buffer, int offset) => Vector.WriteTo(buffer, offset);

			public override void Pack(Packer packer) => packer.PackVector(Vector);

			internal override bool HasVector() => true;

			public override void ValidateKeyType() =>
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: Vector");

			public override string ToString() => Vector.ToString();

			public override bool Equals(object obj)
			{
				if (obj is Vector v) return Equals(v);
				if (obj is VectorValue vv) return Equals(vv);
				return false;
			}

			public bool Equals(VectorValue other) => other is not null && Equals(other.Vector);

			public bool Equals(Vector other) => Vector.Equals(other);

			public override int GetHashCode() => Vector.GetHashCode();

			public static bool operator ==(VectorValue o1, VectorValue o2) => ReferenceEquals(o1, o2) || (o1 is not null && o1.Equals(o2));
			public static bool operator !=(VectorValue o1, VectorValue o2) => !(o1 == o2);

			public static bool operator ==(VectorValue o1, Vector o2) => o1 is null ? o2 is null : o1.Equals(o2);
			public static bool operator !=(VectorValue o1, Vector o2) => !(o1 == o2);
		}
	}
}
