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
    /// <summary>
    /// String value.
    /// </summary>
    public sealed class StringValue : Value, IEquatable<StringValue>, IEquatable<string>
    {
        public string value { get; }

        public override ParticleType Type { get => ParticleType.STRING; }

        public override object Object { get => value; }

        public StringValue(string value)
        {
            this.value = value;
        }

        public override int EstimateSize() => ByteUtil.EstimateSizeUtf8(value);

        public override int Write(byte[] buffer, int offset) => ByteUtil.StringToUtf8(value, buffer, offset);

        public override void Pack(Packer packer) => packer.PackParticleString(value);

        public override string ToString() => value;

        public override int GetHashCode() => value.GetHashCode();

        public override bool Equals(object obj)
        {
            if (obj is string sValue) return Equals(sValue);
            if (obj is StringValue vValue) return Equals(vValue);

            return false;
        }

        public bool Equals(StringValue other) => other is null || other.value is null ? false : value.Equals(other.value);

        public bool Equals(string other) => other is null ? false : value.Equals(other);

        public static bool operator ==(StringValue o1, StringValue o2) => o1?.Equals(o2) ?? false;
        public static bool operator !=(StringValue o1, StringValue o2) => o1 == o2 ? false : true;

        public static bool operator ==(StringValue o1, string o2) => o1?.Equals(o2) ?? false;
        public static bool operator !=(StringValue o1, string o2) => o1 == o2 ? false : true;
    }
}
