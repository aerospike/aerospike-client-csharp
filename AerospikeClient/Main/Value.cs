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
using System;
using System.IO;
using System.Collections;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.SqlTypes;
using System.Runtime.CompilerServices;
using System.Collections.Generic;

namespace Aerospike.Client
{ 	    
    /// <summary>
	/// Polymorphic value structs used to efficiently serialize objects into the wire protocol.
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
        public abstract void Pack(Aerospike.Client.Packer packer);

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
        public virtual int ToInteger()
        {
            return 0;
        }

        /// <summary>
        /// Return value as an unsigned integer.
        /// </summary>
        public virtual uint ToUnsignedInteger()
        {
            return 0;
        }

        /// <summary>
        /// Return value as a long.
        /// </summary>
        public virtual long ToLong()
        {
            return 0;
        }

        /// <summary>
        /// Return value as an unsigned long.
        /// </summary>
        public virtual ulong ToUnsignedLong()
        {
            return 0;
        }

        /// <summary>
		/// Get null value instance.
		/// </summary>
		public static Value AsNull
        {
            get
            {
                return NullValue.Instance;
            }
        }

        /// <summary>
        /// Get null value instance.
        /// </summary>
        public bool IsNull { get => this.Type == Aerospike.Client.ParticleType.NULL; }

        /// <summary>
		/// Get string or null value instance.
		/// </summary>
        static public StringValue Get(string value) => new StringValue(value);

        /// <summary>
        /// Get byte array value instance.
        /// </summary>
        static public BytesValue Get(byte[] value) => new BytesValue(value);

        /// <summary>
        /// Get byte array segment value instance.
        /// </summary>
        static public ByteSegmentValue Get(byte[] value, int offset, int length) => new ByteSegmentValue(value, offset, length);

        /// <summary>
        /// Get double value instance.
        /// </summary>
        static public DoubleValue Get(double value) => new DoubleValue(value);

        /// <summary>
        /// Get float value instance.
        /// </summary>
        static public FloatValue Get(float value) => new FloatValue(value);

        /// <summary>
        /// Get long value instance.
        /// </summary>
        static public LongValue Get(long value) => new LongValue(value);

        /// <summary>
        /// Get unsigned long value instance.
        /// </summary>
        static public UnsignedLongValue Get(ulong value) => new UnsignedLongValue(value);

        /// <summary>
        /// Get integer value instance.
        /// </summary>
        static public IntegerValue Get(int value) => new IntegerValue(value);

        /// <summary>
        /// Get unsigned integer value instance.
        /// </summary>
        static public UnsignedIntegerValue Get(uint value) => new UnsignedIntegerValue(value);

        /// <summary>
        /// Get short value instance.
        /// </summary>
        static public ShortValue Get(short value) => new ShortValue(value);

        /// <summary>
        /// Get unsigned short value instance.
        /// </summary>
        static public UnsignedShortValue Get(ushort value) => new UnsignedShortValue(value);

        /// <summary>
        /// Get boolean value instance.
        /// </summary>
        public static Value Get(bool value)
        {
            if (UseBoolBin)
            {
                return new BooleanValue(value);
            }
            else
            {
                return new BoolIntValue(value);
            }
        }

        /// <summary>
        /// Get boolean value instance.
        /// </summary>
        static public ByteValue Get(byte value) => new ByteValue(value);

        /// <summary>
        /// Get signed boolean value instance.
        /// </summary>
        static public SignedByteValue Get(sbyte value) => new SignedByteValue(value);

        /// <summary>
        /// Get blob value instance.
        /// </summary>
        static public BlobValue Get(BlobValue value) => new BlobValue(value);

        /// <summary>
        /// Get GeoJSON value instance.
        /// </summary>
        /// TODO
        static public GeoJSONValue GetAsGeoJSON(string value) => new GeoJSONValue(value);

        /// <summary>
        /// Get HyperLogLog value instance.
        /// </summary>
        /// TODO
        static public HLLValue GetAsHLL(byte[] value) => new HLLValue(value);

        /// <summary>
        /// Get HyperLogLog value instance.
        /// </summary>
        static public ValueArray Get(Value[] value) => new ValueArray(value);

        /// <summary>
        /// Get list value instance.
        /// </summary>
        static public ListValue Get(IList value) => new ListValue(value);

        /// <summary>
        /// Get map value instance.
        /// </summary>
        static public MapValue Get(IDictionary value) => new MapValue(value);

        /// <summary>
        /// Get map value instance.
        /// </summary>
        static public MapValue Get(IDictionary value, MapOrder order) => new MapValue(value, order);
    }

    public abstract class Value<T> : Value, IEquatable<T>, IEquatable<Value<T>>
    where T : struct
    {
        internal Value(ParticleType type)
        {
            this.Type = type;
            this.value = default(T);
        }

        internal Value(T value, ParticleType type)
        {
            // TODO ask about this vs as null
            this.Type = value.Equals(null) ? Aerospike.Client.ParticleType.NULL : type;
            this.value = value;
        }

        public T value { get; }

        public override ParticleType Type { get; }

        public override object Object { get => this.value; }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            if (obj is Value<T> oValue) return this.Equals(oValue.value);
            if (this.IsNull && ReferenceEquals(obj, null)) return true;

            return false;
        }

        public bool Equals(T other)
        {
            return this.value.Equals(other);
        }

        public bool Equals(Value<T> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;

            return this.value.Equals(other.value);
        }

        public override int GetHashCode()
        {
            if (this.IsNull) return 0;

            return this.value.GetHashCode();
        }
    }

    /// <summary>
    /// Empty value.
    /// </summary>
    public sealed class NullValue : Value
    {
        public static readonly NullValue Instance = new NullValue();

        public override int EstimateSize()
        {
            return 0;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return 0;
        }

        public override void Pack(Packer packer)
        {
            packer.PackNil();
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: null");
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.NULL;
            }
        }

        public override object Object
        {
            get
            {
                return null;
            }
        }

        public override string ToString()
        {
            return null;
        }

        public override int GetHashCode()
        {
            return 0;
        }
    }

    /// <summary>
    /// String value.
    /// </summary>
    /// TODO <string>
    public sealed class StringValue : Value
    {
        private readonly string value;

        public StringValue(string value)
        {
            this.value = value;
        }

        public override int EstimateSize()
        {
            return ByteUtil.EstimateSizeUtf8(value);
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.StringToUtf8(value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackParticleString(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.STRING;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return value;
        }

        public override int GetHashCode()
        {
            return value.GetHashCode();
        }
    }

    /// <summary>
    /// Byte array value.
    /// </summary>
    /// TODO <byte[]>?
    public sealed class BytesValue : Value
    {
        // TODO properties vs fields?
        private readonly byte[] bytes;

        public BytesValue(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public override int EstimateSize()
        {
            return bytes.Length;
        }

        public override int Write(byte[] buffer, int offset)
        {
            Array.Copy(bytes, 0, buffer, offset, bytes.Length);
            return bytes.Length;
        }

        public override void Pack(Packer packer)
        {
            packer.PackParticleBytes(bytes);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.BLOB;
            }
        }

        public override object Object
        {
            get
            {
                return bytes;
            }
        }

        public override string ToString()
        {
            return ByteUtil.BytesToHexString(bytes);
        }

        public override int GetHashCode()
        {
            int result = 1;
            foreach (byte b in bytes)
            {
                result = 31 * result + b;
            }
            return result;
        }
    }

    /// <summary>
    /// Byte segment value.
    /// </summary>
    public sealed class ByteSegmentValue : Value, IEquatable<ByteSegmentValue>
    {
        private readonly byte[] bytes;
        private readonly int offset;
        private readonly int length;

        public ByteSegmentValue(byte[] bytes, int offset, int length)
        {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }

        public override int EstimateSize()
        {
            return length;
        }

        public override int Write(byte[] buffer, int targetOffset)
        {
            Array.Copy(bytes, offset, buffer, targetOffset, length);
            return length;
        }

        public override void Pack(Aerospike.Client.Packer packer)
        {
            packer.PackParticleBytes(bytes, offset, length);
        }

        public override ParticleType Type
        {
            get
            {
                return Aerospike.Client.ParticleType.BLOB;
            }
        }

        public override object Object
        {
            get
            {
                return this;
            }
        }

        public override string ToString()
        {
            return Aerospike.Client.ByteUtil.BytesToHexString(bytes, offset, length);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (obj is ByteSegmentValue bObj)
            {
                return this.Equals(bObj);
            }

            return false;
        }

        public bool Equals(ByteSegmentValue other)
        {
            if (this.length != other.length)
            {
                return false;
            }

            for (int i = 0; i < length; i++)
            {
                if (this.bytes[this.offset + i] != other.bytes[other.offset + i])
                {
                    return false;
                }
            }
            return true;
        }

        public override int GetHashCode()
        {
            int result = 1;
            for (int i = 0; i < length; i++)
            {
                result = 31 * result + bytes[offset + i];
            }
            return result;
        }

        public byte[] Bytes
        {
            get
            {
                return bytes;
            }
        }

        public int Offset
        {
            get
            {
                return offset;
            }
        }

        public int Length
        {
            get
            {
                return length;
            }
        }
    }

    /// <summary>
    /// Double value.
    /// </summary>
    public sealed class DoubleValue : Value<double>
    {
        public DoubleValue(double value)
            : base(value, Aerospike.Client.ParticleType.DOUBLE)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return Aerospike.Client.ByteUtil.DoubleToBytes(value, buffer, offset);
        }

        public override void Pack(Aerospike.Client.Packer packer)
        {
            packer.PackDouble(value);
        }

        public override int GetHashCode()
        {
            ulong bits = (ulong)BitConverter.DoubleToInt64Bits(value);
            return (int)(bits ^ (bits >> 32));
        }

        public override int ToInteger()
        {
            return (int)value;
        }

        public override uint ToUnsignedInteger()
        {
            return (uint)value;
        }

        public override long ToLong()
        {
            return (long)value;
        }

        public override ulong ToUnsignedLong()
        {
            return (ulong)value;
        }
    }

    /// <summary>
    /// Float value.
    /// </summary>
    public sealed class FloatValue : Value<float>
    {
        public FloatValue(float value)
        : base(value, Aerospike.Client.ParticleType.DOUBLE)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.DoubleToBytes(value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackFloat(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.DOUBLE;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override int GetHashCode()
        {
            ulong bits = (ulong)BitConverter.DoubleToInt64Bits(value);
            return (int)(bits ^ (bits >> 32));
        }

        public override int ToInteger()
        {
            return (int)value;
        }

        public override uint ToUnsignedInteger()
        {
            return (uint)value;
        }

        public override long ToLong()
        {
            return (long)value;
        }

        public override ulong ToUnsignedLong()
        {
            return (ulong)value;
        }
    }

    /// <summary>
    /// Long value.
    /// </summary>
    public sealed class LongValue : Value<long>
    {
        public LongValue(long value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes((ulong)value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override int GetHashCode()
        {
            return (int)((ulong)value ^ ((ulong)value >> 32));
        }

        public override int ToInteger()
        {
            return (int)value;
        }

        public override uint ToUnsignedInteger()
        {
            return (uint)value;
        }

        public override long ToLong()
        {
            return value;
        }

        public override ulong ToUnsignedLong()
        {
            return (ulong)value;
        }
    }

    /// <summary>
    /// Unsigned long value.
    /// </summary>
    public sealed class UnsignedLongValue : Value<ulong>
    {
        public UnsignedLongValue(ulong value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return ((value & 0x8000000000000000) == 0) ? 8 : 9;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes(value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((UnsignedLongValue)obj).value);
        }

        public override int GetHashCode()
        {
            return (int)(value ^ (value >> 32));
        }

        public override int ToInteger()
        {
            return (int)value;
        }

        public override uint ToUnsignedInteger()
        {
            return (uint)value;
        }

        public override long ToLong()
        {
            return (long)value;
        }

        public override ulong ToUnsignedLong()
        {
            return value;
        }
    }

    /// <summary>
    /// Integer value.
    /// </summary>
    public sealed class IntegerValue : Value<int>
    {
        public IntegerValue(int value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes((ulong)value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((IntegerValue)obj).value);
        }

        public override int GetHashCode()
        {
            return value;
        }

        public override int ToInteger()
        {
            return value;
        }

        public override uint ToUnsignedInteger()
        {
            return (uint)value;
        }

        public override long ToLong()
        {
            return value;
        }

        public override ulong ToUnsignedLong()
        {
            return (ulong)value;
        }
    }

    /// <summary>
    /// Unsigned integer value.
    /// </summary>
    public sealed class UnsignedIntegerValue : Value<uint>
    {
        public UnsignedIntegerValue(uint value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes(value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((UnsignedIntegerValue)obj).value);
        }

        public override int GetHashCode()
        {
            return (int)value;
        }

        public override int ToInteger()
        {
            return (int)value;
        }

        public override uint ToUnsignedInteger()
        {
            return value;
        }

        public override long ToLong()
        {
            return value;
        }

        public override ulong ToUnsignedLong()
        {
            return value;
        }
    }

    /// <summary>
    /// Short value.
    /// </summary>
    public sealed class ShortValue : Value<short>
    {
        public ShortValue(short value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes((ulong)value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((ShortValue)obj).value);
        }

        public override int GetHashCode()
        {
            return (int)value;
        }

        public override int ToInteger()
        {
            return value;
        }

        public override uint ToUnsignedInteger()
        {
            return (uint)value;
        }

        public override long ToLong()
        {
            return value;
        }

        public override ulong ToUnsignedLong()
        {
            return (ulong)value;
        }
    }

    /// <summary>
    /// Unsigned short value.
    /// </summary>
    public sealed class UnsignedShortValue : Value<ushort>
    {
        public UnsignedShortValue(ushort value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes(value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((UnsignedShortValue)obj).value);
        }

        public override int GetHashCode()
        {
            return (int)value;
        }

        public override int ToInteger()
        {
            return value;
        }

        public override uint ToUnsignedInteger()
        {
            return value;
        }

        public override long ToLong()
        {
            return value;
        }

        public override ulong ToUnsignedLong()
        {
            return value;
        }
    }

    /// <summary>
    /// Boolean value.
    /// </summary>
    public sealed class BooleanValue : Value<bool>
    {
        public BooleanValue(bool value)
        : base(value, Aerospike.Client.ParticleType.BOOL)
        {
        }

        public override int EstimateSize()
        {
            return 1;
        }

        public override int Write(byte[] buffer, int offset)
        {
            buffer[offset] = value ? (byte)1 : (byte)0;
            return 1;
        }

        public override void Pack(Packer packer)
        {
            packer.PackBoolean(value);
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: bool");
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.BOOL;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((BooleanValue)obj).value);
        }

        public override int GetHashCode()
        {
            return value ? 1231 : 1237;
        }

        public override int ToInteger()
        {
            return value ? 1 : 0;
        }

        public override uint ToUnsignedInteger()
        {
            return value ? (uint)1 : (uint)0;
        }

        public override long ToLong()
        {
            return value ? 1 : 0;
        }

        public override ulong ToUnsignedLong()
        {
            return value ? (ulong)1 : (ulong)0;
        }
    }

    /// <summary>
    /// Boolean value that converts to integer when sending a bin to the server.
    /// This class will be deleted once full conversion to boolean particle type
    /// is complete.
    /// </summary>
    public sealed class BoolIntValue : Value, IEquatable<BoolIntValue>
    {
        private readonly bool value;

        public BoolIntValue(bool value)
        {
            this.value = value;
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes(value ? 1UL : 0UL, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackBoolean(value);
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: BoolIntValue");
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (obj is BoolIntValue bObj)
            {
                return this.Equals(bObj);
            }

            return false;
        }

        public bool Equals(BoolIntValue other)
        {
            return this.value.Equals(other);
        }


        public override int GetHashCode()
        {
            return value ? 1231 : 1237;
        }

        public override int ToInteger()
        {
            return value ? 1 : 0;
        }

        public override uint ToUnsignedInteger()
        {
            return value ? (uint)1 : (uint)0;
        }

        public override long ToLong()
        {
            return value ? 1 : 0;
        }

        public override ulong ToUnsignedLong()
        {
            return value ? (ulong)1 : (ulong)0;
        }
    }

    /// <summary>
    /// Byte value.
    /// </summary>
    public sealed class ByteValue : Value<byte>
    {
        public ByteValue(byte value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes((ulong)value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((ByteValue)obj).value);
        }

        public override int GetHashCode()
        {
            return (int)value;
        }

        public override int ToInteger()
        {
            return value;
        }

        public override uint ToUnsignedInteger()
        {
            return value;
        }

        public override long ToLong()
        {
            return value;
        }

        public override ulong ToUnsignedLong()
        {
            return value;
        }
    }

    /// <summary>
    /// Byte value.
    /// </summary>
    public sealed class SignedByteValue : Value<sbyte>
    {
        public SignedByteValue(sbyte value)
        : base(value, Aerospike.Client.ParticleType.INTEGER)
        {
        }

        public override int EstimateSize()
        {
            return 8;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return ByteUtil.LongToBytes((ulong)value, buffer, offset);
        }

        public override void Pack(Packer packer)
        {
            packer.PackNumber(value);
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.INTEGER;
            }
        }

        public override object Object
        {
            get
            {
                return value;
            }
        }

        public override string ToString()
        {
            return Convert.ToString(value);
        }

        public override bool Equals(object obj)
        {
            return (obj != null &&
                this.GetType().Equals(obj.GetType()) &&
                this.value == ((SignedByteValue)obj).value);
        }

        public override int GetHashCode()
        {
            return (int)value;
        }

        public override int ToInteger()
        {
            return value;
        }

        public override uint ToUnsignedInteger()
        {
            return (uint)value;
        }

        public override long ToLong()
        {
            return value;
        }

        public override ulong ToUnsignedLong()
        {
            return (ulong)value;
        }
    }

    /// <summary>
    /// Blob value.
    /// </summary>
    public sealed class BlobValue : IEquatable<BlobValue>
    {
        public object Obj { get; }

        public byte[] Bytes { get; set; }

        public ParticleType Type
        {
            get
            {
                return ParticleType.CSHARP_BLOB;
            }
        }

        public BlobValue(object obj)
        {
            this.Obj = obj;
            this.Bytes = default;
        }

        public int EstimateSize()
        {
            this.Bytes = Serialize(Obj);
            return Bytes.Length;
        }

        public static byte[] Serialize(object val)
        {
#if BINARY_FORMATTER
				if (DisableSerializer)
				{
					throw new AerospikeException("Object serializer has been disabled");
				}

				using (MemoryStream ms = new MemoryStream())
				{
					BinaryFormatter formatter = new BinaryFormatter();
					formatter.Serialize(ms, val);
					return ms.ToArray();
				}
#else
            throw new AerospikeException("Object serializer has been disabled");
#endif
        }

        public int Write(byte[] buffer, int offset)
        {
            Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
            return Bytes.Length;
        }

        public void Pack(Packer packer)
        {
            // Do not try to pack bytes field because it will be null
            // when packing objects in a collection (ie. EstimateSize() not called).
            packer.PackBlob(Obj);
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: csblob");
        }

        public override string ToString()
        {
            return ByteUtil.BytesToHexString(Bytes);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(BlobValue) == obj.GetType())
            {
                return this.Equals((BlobValue)obj);
            }

            return false;
        }

        public bool Equals(BlobValue other)
        {
            for (int i = 0; i < this.Bytes.Length; i++)
            {
                if (this.Bytes[i] != other.Bytes[i])
                {
                    return false;
                }
            }
            return true;
        }

        public override int GetHashCode()
        {
            return Obj.GetHashCode();
        }
    }

    /// <summary>
    /// GeoJSON value.
    /// </summary>
    public sealed class GeoJSONValue : IEquatable<GeoJSONValue>
    {
        public string value { get; }

        public ParticleType Type
        {
            get
            {
                return ParticleType.GEOJSON;
            }
        }

        public GeoJSONValue(string value)
        {
            this.value = value;
        }

        public int EstimateSize()
        {
            // flags + ncells + jsonstr
            return 1 + 2 + ByteUtil.EstimateSizeUtf8(value);
        }

        public int Write(byte[] buffer, int offset)
        {
            buffer[offset] = 0; // flags
            ByteUtil.ShortToBytes(0, buffer, offset + 1); // ncells
            return 1 + 2 + ByteUtil.StringToUtf8(value, buffer, offset + 3); // jsonstr
        }

        public void Pack(Packer packer)
        {
            packer.PackGeoJSON(value);
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: GeoJSON");
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(GeoJSONValue) == obj.GetType())
            {
                return this.Equals((GeoJSONValue)obj);
            }

            return false;
        }

        public bool Equals(GeoJSONValue other)
        {
            return this.value.Equals(other.value);
        }

        public override int GetHashCode()
        {
            return value.GetHashCode();
        }
    }

    /// <summary>
    /// HyperLogLog value.
    /// </summary>
    public sealed class HLLValue : Value, IEquatable<HLLValue>
    {
        private readonly byte[] bytes;

        public HLLValue(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public override int EstimateSize()
        {
            return bytes.Length;
        }

        public override int Write(byte[] buffer, int offset)
        {
            Array.Copy(bytes, 0, buffer, offset, bytes.Length);
            return bytes.Length;
        }

        public override void Pack(Packer packer)
        {
            packer.PackParticleBytes(bytes);
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: HLL");
        }

        public override ParticleType Type
        {
            get { return ParticleType.HLL; }
        }

        public override object Object
        {
            get { return bytes; }
        }

        public byte[] Bytes
        {
            get { return bytes; }
        }

        public override string ToString()
        {
            return ByteUtil.BytesToHexString(Bytes);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(HLLValue) == obj.GetType())
            {
                return this.Equals((HLLValue)obj);
            }

            return false;
        }

        public bool Equals(HLLValue other)
        {
            return Util.ByteArrayEquals(this.Bytes, other.Bytes);
        }

        public override int GetHashCode()
        {
            int result = 1;
            foreach (byte b in Bytes)
            {
                result = 31 * result + b;
            }
            return result;
        }
    }

    /// <summary>
    /// Value array.
    /// </summary>
    public sealed class ValueArray : Value, IEquatable<ValueArray>
    {
        private readonly Value[] array;
        private byte[] bytes;

        public ValueArray(Value[] array)
        {
            this.array = array;
        }

        public override int EstimateSize()
        {
            bytes = Aerospike.Client.Packer.Pack(array);
            return bytes.Length;
        }

        public override int Write(byte[] buffer, int offset)
        {
            Array.Copy(bytes, 0, buffer, offset, bytes.Length);
            return bytes.Length;
        }

        public override void Pack(Aerospike.Client.Packer packer)
        {
            packer.PackValueArray(array);
        }

        public override void ValidateKeyType()
        {
            throw new Aerospike.Client.AerospikeException(Aerospike.Client.ResultCode.PARAMETER_ERROR, "Invalid key type: value[]");
        }

        public override ParticleType Type
        {
            get
            {
                return Aerospike.Client.ParticleType.LIST;
            }
        }

        public override object Object
        {
            get
            {
                return array;
            }
        }

        public override string ToString()
        {
            return Aerospike.Client.Util.ArrayToString(array);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(ValueArray) == obj.GetType())
            {
                return this.Equals((ValueArray)obj);
            }

            return false;
        }

        public bool Equals(ValueArray other)
        {
            if (this.array.Length != other.array.Length)
            {
                return false;
            }

            for (int i = 0; i < this.array.Length; i++)
            {
                Value v1 = this.array[i];
                Value v2 = other.array[i];

                if (v1 == null)
                {
                    if (v2 == null)
                    {
                        continue;
                    }
                    return false;
                }

                if (!v1.Equals(v2))
                {
                    return false;
                }
            }
            return true;
        }

        public override int GetHashCode()
        {
            int result = 1;
            foreach (Value item in array)
            {
                result = 31 * result + (item == null ? 0 : item.GetHashCode());
            }
            return result;
        }
    }

    /// <summary>
    /// List value.
    /// </summary>
    public sealed class ListValue : Value, IEquatable<ListValue>
    {
        internal readonly IList list;
        internal byte[] bytes;

        public ListValue(IList list)
        {
            this.list = list;
            this.bytes = default(byte[]);
        }

        public override int EstimateSize()
        {
            bytes = Packer.Pack(list);
            return bytes.Length;
        }

        public override int Write(byte[] buffer, int offset)
        {
            Array.Copy(bytes, 0, buffer, offset, bytes.Length);
            return bytes.Length;
        }

        public override void Pack(Packer packer)
        {
            packer.PackList(list);
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: list");
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.LIST;
            }
        }

        public override object Object
        {
            get
            {
                return list;
            }
        }

        public override string ToString()
        {
            return list.ToString();
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(ListValue) == obj.GetType())
            {
                return this.Equals((ListValue)obj);
            }

            return false; 
        }

        public bool Equals(ListValue other)
        {
            if (this.list.Count != other.list.Count)
            {
                return false;
            }

            for (int i = 0; i < this.list.Count; i++)
            {
                object v1 = this.list[i];
                object v2 = other.list[i];

                if (v1 == null)
                {
                    if (v2 == null)
                    {
                        continue;
                    }
                    return false;
                }

                if (!v1.Equals(v2))
                {
                    return false;
                }
            }
            return true;
        }

        public override int GetHashCode()
        {
            int result = 1;
            foreach (object value in list)
            {
                result = 31 * result + (value == null ? 0 : value.GetHashCode());
            }
            return result;
        }
    }

    /// <summary>
    /// Map value.
    /// </summary>
    public sealed class MapValue : Value, IEquatable<MapValue>
    {
        internal readonly IDictionary map;
        internal readonly MapOrder order;
        internal byte[] bytes;

        public MapValue(IDictionary map)
        {
            this.map = map;
            this.order = MapOrder.UNORDERED;
            this.bytes = default(byte[]);
        }

        public MapValue(IDictionary map, MapOrder order)
        {
            this.map = map;
            this.order = order;
            this.bytes = default(byte[]);
        }

        public MapOrder MapOrder
        {
            get { return MapOrder; }
        }

        public override int EstimateSize()
        {
            bytes = Packer.Pack(map, order);
            return bytes.Length;
        }

        public override int Write(byte[] buffer, int offset)
        {
            Array.Copy(bytes, 0, buffer, offset, bytes.Length);
            return bytes.Length;
        }

        public override void Pack(Packer packer)
        {
            packer.PackMap(map, order);
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: map");
        }

        public override ParticleType Type
        {
            get
            {
                return ParticleType.MAP;
            }
        }

        public override object Object
        {
            get
            {
                return map;
            }
        }

        public override string ToString()
        {
            return map.ToString();
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(MapValue) == obj.GetType())
            {
                return this.Equals((MapValue)obj);
            }

            return false;
        }

        public bool Equals(MapValue other)
        {
            if (this.map.Count != other.map.Count)
            {
                return false;
            }

            try
            {
                foreach (DictionaryEntry entry in this.map)
                {
                    object v1 = entry.Value;
                    object v2 = other.map[entry.Key];

                    if (v1 == null)
                    {
                        if (v2 == null)
                        {
                            continue;
                        }
                        return false;
                    }

                    if (!v1.Equals(v2))
                    {
                        return false;
                    }
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
            foreach (DictionaryEntry entry in map)
            {
                result = 31 * result + (entry.Key == null ? 0 : entry.Key.GetHashCode());
                result = 31 * result + (entry.Value == null ? 0 : entry.Value.GetHashCode());
            }
            return result;
        }
    }

    /// <summary>
    /// Infinity value.
    /// </summary>
    public sealed class InfinityValue : Value
    {
        public override int EstimateSize()
        {
            return 0;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return 0;
        }

        public override void Pack(Packer packer)
        {
            packer.PackInfinity();
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: INF");
        }

        public override ParticleType Type
        {
            get
            {
                throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid particle type: INF");
            }
        }

        public override object Object
        {
            get
            {
                return null;
            }
        }

        public override string ToString()
        {
            return "INF";
        }

        public override bool Equals(object other)
        {
            return (other != null && this.GetType().Equals(other.GetType()));
        }

        public override int GetHashCode()
        {
            return 0;
        }
    }

    /// <summary>
    /// Wildcard value.
    /// </summary>
    public sealed class WildcardValue : Value
    {
        public override int EstimateSize()
        {
            return 0;
        }

        public override int Write(byte[] buffer, int offset)
        {
            return 0;
        }

        public override void Pack(Packer packer)
        {
            packer.PackWildcard();
        }

        public override void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: wildcard");
        }

        public override ParticleType Type
        {
            get
            {
                throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid particle type: wildcard");
            }
        }

        public override object Object
        {
            get
            {
                return null;
            }
        }

        public override string ToString()
        {
            return "*";
        }

        public override bool Equals(object other)
        {
            return (other != null && this.GetType().Equals(other.GetType()));
        }

        public override int GetHashCode()
        {
            return 0;
        }
    }
}
