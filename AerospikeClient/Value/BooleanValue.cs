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
		/// Boolean value.
		/// </summary>
		public sealed class BooleanValue : Value<bool>
		{
			public BooleanValue(bool value)
			: base(value, ParticleType.BOOL)
			{
			}

			public override int EstimateSize() => 1;

			public override int Write(byte[] buffer, int offset)
			{
				buffer[offset] = value ? (byte)1 : (byte)0;
				return 1;
			}

			public override void Pack(Packer packer) => packer.PackBoolean(value);

			public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: bool");

			public override int GetHashCode() => value ? 1231 : 1237;

			public override int ToInteger() => value ? 1 : 0;

			public override uint ToUnsignedInteger() => value ? 1 : (uint)0;

			public override long ToLong() => value ? 1 : 0;

			public override ulong ToUnsignedLong() => value ? 1 : (ulong)0;
		}
	}
}
