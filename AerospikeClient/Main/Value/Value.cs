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
    /// <summary>
    /// Polymorphic value classes used to efficiently serialize objects into the wire protocol.
    /// </summary>
    public abstract class Value
    {
        /// <summary>
		/// Should client send boolean particle type for a boolean bin.  If false,
		/// an integer particle type (1 or 0) is sent instead. Must be false for server
		/// versions less than 5.6 which do not support boolean bins. Can set to true for
		/// server 5.6+.
		/// </summary>
		public static bool UseBoolBin = false;

        /// <summary>
		/// Should BinaryFormatter be disabled. If true, an exception will be thrown when BinaryFormatter
		/// is used. BinaryFormatter has been removed from the client by default, so this field is no
		/// longer relevant.
		/// </summary>
		public static bool DisableSerializer = false;

        /// <summary>
        /// Should default object deserializer be disabled. If true, an exception will be thrown when
        /// a default object deserialization is attempted. Default object serialization is triggered
        /// when serialized data is read/parsed from the server. DisableDeserializer is separate from
        /// DisableSerializer because there may be cases when no new serialization is allowed, but
        /// existing serialized objects need to be supported. BinaryFormatter has been removed from
        /// the client by default, so this field is no longer relevant.
        /// </summary>
        public static bool DisableDeserializer = false;

        /// <summary>
        /// Get wire protocol value type.
        /// </summary>
        public abstract ParticleType Type { get; }

        /// <summary>
        /// Return original value as an Object.
        /// </summary>
        public abstract object Object { get; }

        /// <summary>
        /// Infinity value to be used in CDT range comparisons only.
        /// </summary>
        public static readonly Value INFINITY = new InfinityValue();

        /// <summary>
        /// Wildcard value to be used in CDT range comparisons only.
        /// </summary>
        public static readonly Value WILDCARD = new WildcardValue();

        /// <summary>
        /// Get null value instance.
        /// </summary>
        public static Value AsNull { get => NullValue.Instance; }

        /// <summary>
        /// Get null value instance.
        /// </summary>
        public bool IsNull { get => Type == ParticleType.NULL; }

        /// <summary>
        /// Calculate number of bytes necessary to serialize the fixed value in the wire protocol.
        /// </summary>
        public abstract int EstimateSize();

        /// <summary>
        /// Serialize the fixed value in the wire protocol.
        /// </summary>
        public abstract int Write(byte[] buffer, int offset);

        /// <summary>
        /// Serialize the value using MessagePack.
        /// </summary>
        public abstract void Pack(Packer packer);

        /// <summary>
        /// Validate if value type can be used as a key.
        /// </summary>
        /// <exception cref="AerospikeException">if type can't be used as a key.</exception>
        public virtual void ValidateKeyType() 
        {
        }

        /// <summary>
        /// Return value as an integer.
        /// </summary>
        public virtual int ToInteger() => 0;

        /// <summary>
        /// Return value as an unsigned integer.
        /// </summary>
        public virtual uint ToUnsignedInteger() => 0;

        /// <summary>
        /// Return value as a long.
        /// </summary>
        public virtual long ToLong() => 0;

        /// <summary>
        /// Return value as an unsigned long.
        /// </summary>
        public virtual ulong ToUnsignedLong() => 0;

        #region Get Methods
        /// <summary>
        /// Get string or null value instance.
        /// </summary>
        static public Value Get(string value) => value is null ? NullValue.Instance : new StringValue(value);

        /// <summary>
        /// Get byte array value instance.
        /// </summary>
        static public Value Get(byte[] value) => value is null ? NullValue.Instance : new BytesValue(value);

        /// <summary>
        /// Get byte array segment value instance.
        /// </summary>
        static public Value Get(byte[] value, int offset, int length) => value is null ? NullValue.Instance : new ByteSegmentValue(value, offset, length);

        /// <summary>
        /// Get double value instance.
        /// </summary>
        static public DoubleValue Get(double value) => new(value);

        /// <summary>
        /// Get float value instance.
        /// </summary>
        static public FloatValue Get(float value) => new(value);

        /// <summary>
        /// Get long value instance.
        /// </summary>
        static public LongValue Get(long value) => new(value);

        /// <summary>
        /// Get unsigned long value instance.
        /// </summary>
        static public UnsignedLongValue Get(ulong value) => new(value);

        /// <summary>
        /// Get integer value instance.
        /// </summary>
        static public IntegerValue Get(int value) => new(value);

        /// <summary>
        /// Get unsigned integer value instance.
        /// </summary>
        static public UnsignedIntegerValue Get(uint value) => new(value);

        /// <summary>
        /// Get short value instance.
        /// </summary>
        static public ShortValue Get(short value) => new(value);

        /// <summary>
        /// Get unsigned short value instance.
        /// </summary>
        static public UnsignedShortValue Get(ushort value) => new(value);

        /// <summary>
        /// Get boolean value instance.
        /// </summary>
        public static Value Get(bool value) => UseBoolBin ? new BooleanValue(value) : new BoolIntValue(value);

        /// <summary>
        /// Get boolean value instance.
        /// </summary>
        static public ByteValue Get(byte value) => new(value);

        /// <summary>
        /// Get signed boolean value instance.
        /// </summary>
        static public SignedByteValue Get(sbyte value) => new(value);

        /// <summary>
        /// Get blob value instance.
        /// </summary>
        static public Value Get(BlobValue value) => value is null ? NullValue.Instance : new BlobValue(value);

        /// <summary>
        /// Get GeoJSON value instance.
        /// </summary>
        static public Value GetAsGeoJSON(string value) => value is null ? NullValue.Instance : new GeoJSONValue(value);

        /// <summary>
        /// Get HyperLogLog value instance.
        /// </summary>
        static public Value GetAsHLL(byte[] value) => value is null ? NullValue.Instance : new HLLValue(value);

        /// <summary>
        /// Get ValueArray value instance.
        /// </summary>
        static public Value Get(Value[] value) => value is null ? NullValue.Instance : new ValueArray(value);

        /// <summary>
        /// Get list value instance.
        /// </summary>
        static public Value Get(IList value) => value is null ? NullValue.Instance : new ListValue(value);

        /// <summary>
        /// Get map value instance.
        /// </summary>
        static public Value Get(IDictionary value) => value is null ? NullValue.Instance : new MapValue(value);

        /// <summary>
        /// Get map value instance.
        /// </summary>
        static public Value Get(IDictionary value, MapOrder order) => value is null ? NullValue.Instance : new MapValue(value, order);

        /// <summary>
		/// Determine value given generic object.
		/// This is the slowest of the Value get() methods.
		/// Useful when copying records from one cluster to another.
		/// </summary>
        static public Value Get<T>(T obj)
        {
            if (obj is null) return NullValue.Instance;

            return obj switch
            {
                Value value => value,
                byte[] bValue => new BytesValue(bValue),
                IList lValue => new ListValue(lValue),
                IDictionary dValue => new MapValue(dValue),
                string sValue => new StringValue(sValue),
                double dnValue => new DoubleValue(dnValue),
                float fnValue => new FloatValue(fnValue),
                long lnValue => new LongValue(lnValue),
                int inValue => new IntegerValue(inValue),
                short snValue => new ShortValue(snValue),
                ulong ulnValue => new UnsignedLongValue(ulnValue),
                uint uinValue => new UnsignedIntegerValue(uinValue),
                ushort usnValue => new UnsignedShortValue(usnValue),
                bool bValue => UseBoolBin ? new BooleanValue(bValue) : new BoolIntValue(bValue),
                byte byValue => new ByteValue(byValue),
                sbyte sbValue => new SignedByteValue(sbValue),
                _ => new BlobValue(obj),
            };
        }
        #endregion
    }

    public abstract class Value<T> : Value, IEquatable<T>, IEquatable<Value<T>>
    where T : struct
    {
        internal Value(ParticleType type)
        {
            Type = type;
            value = default;
        }

        internal Value(T value, ParticleType type)
        {
            Type = type;
            this.value = value;
        }

        public T value { get; }

        public override ParticleType Type { get; }

        public override object Object { get => value; }

        public override string ToString() => Convert.ToString(value);

        public override bool Equals(object obj)
        {
            if (obj is Value<T> oValue) return Equals(oValue.value);
            if (IsNull && obj is null) return true;

            return false;
        }

        public bool Equals(T other) => value.Equals(other);

        public bool Equals(Value<T> other)
        {
            if (IsNull || other.IsNull) return IsNull && other.IsNull;

            return value.Equals(other.value);
        }

        public override int GetHashCode()
        {
            if (IsNull) return 0;

            return value.GetHashCode();
        }

        public static bool operator ==(Value<T> o1, Value<T> o2) => o1?.Equals(o2) ?? false;
        public static bool operator !=(Value<T> o1, Value<T> o2) => o1 == o2 ? false : true;

        public static bool operator ==(Value<T> o1, T o2) => o1?.Equals(o2) ?? false;
        public static bool operator !=(Value<T> o1, T o2) => o1 == o2 ? false : true;
    }
}
