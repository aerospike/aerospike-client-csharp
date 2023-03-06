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
		/// Wildcard value.
		/// </summary>
		public sealed class WildcardValue : Value
		{
			public override ParticleType Type
			{
				get => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid particle type: wildcard");
			}

			public override object Object { get => null; }

			public override int EstimateSize() => 0;

			public override int Write(byte[] buffer, int offset) => 0;

			public override void Pack(Packer packer) => packer.PackWildcard();

			public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: wildcard");


			public override string ToString() => "*";

			public override bool Equals(object obj)
			{
				if (obj is WildcardValue) return true;

				return false;
			}

			public override int GetHashCode() => 0;
		}
	}
}
