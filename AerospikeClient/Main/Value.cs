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

namespace Aerospike.Client
{ 	
	public static class Value
    {
        /// <summary>
		/// Get string or null value instance.
		/// </summary>
        public static Value<string> Get(this string value) => value == null ? new Value<string>(value, ParticleType.NULL) : new Value<string>(value, ParticleType.STRING);

        /// <summary>
        /// Get byte array value instance.
        /// </summary>
        public static Value<byte[]> Get(this byte[] value) => new Value<byte[]>(value, ParticleType.BLOB);

        /// <summary>
        /// Get byte array segment value instance.
        /// </summary>
        public static Value<ByteSegmentValue> Get(this ByteSegmentValue value) => new Value<ByteSegmentValue>(value, ParticleType.BLOB);

        /// <summary>
        /// Get double value instance.
        /// </summary>
        public static Value<double> Get(this double value) => new Value<double>(value, ParticleType.DOUBLE);

        /// <summary>
        /// Get float value instance.
        /// </summary>
        public static Value<float> Get(this float value) => new Value<float>(value, ParticleType.DOUBLE);

        /// <summary>
        /// Get long value instance.
        /// </summary>
        public static Value<long> Get(this long value) => new Value<long>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get unsigned long value instance.
        /// </summary>
        public static Value<ulong> Get(this ulong value) => new Value<ulong>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get integer value instance.
        /// </summary>
        public static Value<int> Get(this int value) => new Value<int>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get unsigned integer value instance.
        /// </summary>
        public static Value<uint> Get(this uint value) => new Value<uint>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get short value instance.
        /// </summary>
        public static Value<short> Get(this short value) => new Value<short>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get unsigned short value instance.
        /// </summary>
        public static Value<ushort> Get(this ushort value) => new Value<ushort>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get boolean value instance.
        /// </summary>
        public static Value<bool> Get(this bool value) => new Value<bool>(value, ParticleType.BOOL);

        /// <summary>
        /// Get boolean value instance.
        /// </summary>
        public static Value<byte> Get(this byte value) => new Value<byte>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get signed boolean value instance.
        /// </summary>
        public static Value<sbyte> Get(this sbyte value) => new Value<sbyte>(value, ParticleType.INTEGER);

        /// <summary>
        /// Get blob value instance.
        /// </summary>
        public static Value<BlobValue> Get(this BlobValue value) => new Value<BlobValue>(value, ParticleType.CSHARP_BLOB);

        /// <summary>
        /// Get GeoJSON value instance.
        /// </summary>
        public static Value<GeoJSONValue> Get(this GeoJSONValue value) => new Value<GeoJSONValue>(value, ParticleType.GEOJSON);

        /// <summary>
        /// Get HyperLogLog value instance.
        /// </summary>
        public static Value<HLLValue> Get(this HLLValue value) => new Value<HLLValue>(value, ParticleType.HLL);

        /// <summary>
        /// Get list value instance.
        /// </summary>
        public static Value<ListValue> Get(this ListValue value) => new Value<ListValue>(value, ParticleType.LIST);

        /// <summary>
        /// Get map value instance.
        /// </summary>
        public static Value<MapValue> Get(this MapValue value) => new Value<MapValue>(value, ParticleType.MAP);

        /// <summary>
		/// Get null value instance.
		/// </summary>
        public static Value<NullValue> AsNull
        {
            get => Value<NullValue>.NULL;
        }
    }
    
    /// <summary>
	/// Polymorphic value structs used to efficiently serialize objects into the wire protocol.
	/// </summary>
	public struct Value<T> : IEquatable<Value<byte[]>>,
							 IEquatable<Value<ByteSegmentValue>>,
                             IEquatable<Value<BlobValue>>,
                             IEquatable<Value<GeoJSONValue>>,
                             IEquatable<Value<HLLValue>>,
                             IEquatable<Value<ValueArray<T>>>,
                             IEquatable<Value<ListValue>>,
                             IEquatable<Value<MapValue>>
    {
        /// <summary>
        /// Get null value instance.
        /// </summary>
        public static Value<NullValue> NULL { get => new Value<NullValue>(ParticleType.NULL); }

        /// <summary>
		/// Get value array instance.
		/// </summary>
        public static Value<ValueArray<T>> Get(ValueArray<T> value) => new Value<ValueArray<T>>(value, ParticleType.LIST);

        /// <summary>
		/// Get wire protocol value type.
		/// </summary>
        public int Type { get; }

        public T value { get; }

        public bool IsNull { get => this.Type == ParticleType.NULL; }

        internal Value(int type)
		{
			this.Type = type;
			this.value = default;
		}

		public Value(T value, int type)
		{
			this.Type = ReferenceEquals(value, null) ? ParticleType.NULL : type ;
			this.value = value;
		}

		public override string ToString()
		{
            if (this.IsNull) return null;
            
            switch (this.value)
            {
                case byte[] bValue:
                    return ToString(bValue);
                default:
                    break;
            }
            
            return this.value?.ToString();
        }

        static public string ToString(byte[] value) => ByteUtil.BytesToHexString(value);

        /// <summary>
        /// Calculate number of bytes necessary to serialize the fixed value in the wire protocol.
        /// </summary>
        public int EstimateSize()
        {
            if (this.IsNull) return 0;

            switch (this.value)
            {
                case string sValue:
                    return EstimateSize(sValue);
                case byte[] bValue:
                    return EstimateSize(bValue);
                case ByteSegmentValue bsValue:
                    return EstimateSize(bsValue);
                case ulong ulValue:
                    return EstimateSize(ulValue);
                case bool boolValue:
                    return 1;
                case BlobValue blobValue:
                    return EstimateSize(blobValue);
                case GeoJSONValue geoJSONValue:
                    return EstimateSize(geoJSONValue);
                case HLLValue hLLValue:
                    return EstimateSize(hLLValue);
                case ValueArray<T> valueArrayValue:
                    return EstimateSize(valueArrayValue);
                case ListValue listValue:
                    return EstimateSize(listValue);
                case MapValue mapValue:
                    return EstimateSize(mapValue);
                default:
                    break;
            }

            return 8;
        }

        static public int EstimateSize(string value) => ByteUtil.EstimateSizeUtf8(value);

        static public int EstimateSize(byte[] value) => value.Length;

        static public int EstimateSize(ByteSegmentValue value) => value.EstimateSize();

        static public int EstimateSize(ulong value) => ((value & 0x8000000000000000) == 0) ? 8 : 9;

        static public int EstimateSize(BlobValue value) => value.EstimateSize();

        static public int EstimateSize(GeoJSONValue value) => value.EstimateSize();

        static public int EstimateSize(HLLValue value) => value.EstimateSize();

        static public int EstimateSize(ValueArray<T> value) => value.EstimateSize();

        static public int EstimateSize(ListValue value) => value.EstimateSize();

        static public int EstimateSize(MapValue value) => value.EstimateSize();

        /// <summary>
		/// Serialize the fixed value in the wire protocol.
		/// </summary>
        public int Write(byte[] buffer, int offset)
        {
            if (this.IsNull) return 0;
            switch (this.value)
            {
                case string sValue:
                    return Write(sValue, buffer, offset);
                case byte[] bValue:
                    return Write(bValue, buffer, offset);
                case ByteSegmentValue bsValue:
                    return Write(bsValue, buffer, offset);
                case double dValue:
                    return ByteUtil.DoubleToBytes(dValue, buffer, offset);
                case float fValue:
                    return ByteUtil.DoubleToBytes(fValue, buffer, offset);
                case long lValue:
                    return ByteUtil.LongToBytes((ulong)lValue, buffer, offset);
                case ulong ulValue:
                    return ByteUtil.LongToBytes(ulValue, buffer, offset);
                case int iValue:
                    return ByteUtil.LongToBytes((ulong)iValue, buffer, offset);
                case uint uiValue:
                    return ByteUtil.LongToBytes(uiValue, buffer, offset);
                case short shValue:
                    return ByteUtil.LongToBytes((ulong)shValue, buffer, offset);
                case ushort ushValue:
                    return ByteUtil.LongToBytes(ushValue, buffer, offset);
                case bool boolValue:
                    return Write(boolValue, buffer, offset);
                case byte byteValue:
                    return ByteUtil.LongToBytes((ulong)byteValue, buffer, offset);
                case sbyte sbyteValue:
                    return ByteUtil.LongToBytes((ulong)sbyteValue, buffer, offset);
                case BlobValue blobValue:
                    return Write(blobValue, buffer, offset);
                case GeoJSONValue geoJSONValue:
                    return Write(geoJSONValue, buffer, offset);
                case HLLValue hLLValue:
                    return Write(hLLValue, buffer, offset);
                case ValueArray<T> valueArrayValue:
                    return Write(valueArrayValue, buffer, offset);
                case ListValue listValue:
                    return Write(listValue, buffer, offset);
                case MapValue mapValue:
                    return Write(mapValue, buffer, offset);
                default:
                    break;
            }

            return 0;
        }

        static public int Write(string value, byte[] buffer, int offset) => ByteUtil.StringToUtf8(value, buffer, offset);

        static public int Write(byte[] value, byte[] buffer, int offset)
        {
			Array.Copy(value, 0, buffer, offset, value.Length);
			return value.Length;
		}

        static public int Write(ByteSegmentValue value, byte[] buffer, int targetOffset) => value.Write(buffer, targetOffset);

        static public int Write(bool value, byte[] buffer, int offset)
        {
            buffer[offset] = value ? (byte)1 : (byte)0;
            return 1;
        }

        static public int Write(BlobValue value, byte[] buffer, int targetOffset) => value.Write(buffer, targetOffset);

        static public int Write(GeoJSONValue value, byte[] buffer, int targetOffset) => value.Write(buffer, targetOffset);

        static public int Write(HLLValue value, byte[] buffer, int targetOffset) => value.Write(buffer, targetOffset);

        static public int Write(ValueArray<T> value, byte[] buffer, int targetOffset) => value.Write(buffer, targetOffset);

        static public int Write(ListValue value, byte[] buffer, int targetOffset) => value.Write(buffer, targetOffset);

        static public int Write(MapValue value, byte[] buffer, int targetOffset) => value.Write(buffer, targetOffset);

        /// <summary>
		/// Serialize the value using MessagePack.
		/// </summary>
        public void Pack(Packer packer)
        {
            if (this.IsNull) packer.PackNil();
            switch (this.value)
            {
                case string sValue:
                    packer.PackParticleString(sValue);
                    break;
                case byte[] bValue:
                    packer.PackParticleBytes(bValue);
                    break;
                case ByteSegmentValue bsValue:
                    Pack(bsValue, packer);
                    break;
                case double dValue:
                    packer.PackDouble(dValue);
                    break;
                case float fValue:
                    packer.PackFloat(fValue);
                    break;
                case long lValue:
                    packer.PackNumber(lValue);
                    break;
                case ulong ulValue:
                    packer.PackNumber(ulValue);
                    break;
                case int iValue:
                    packer.PackNumber(iValue);
                    break;
                case uint uiValue:
                    packer.PackNumber(uiValue);
                    break;
                case short shValue:
                    packer.PackNumber(shValue);
                    break;
                case ushort ushValue:
                    packer.PackNumber(ushValue);
                    break;
                case bool boolValue:
                    packer.PackBoolean(boolValue);
                    break;
                case byte byteValue:
                    packer.PackNumber(byteValue);
                    break;
                case sbyte sbyteValue:
                    packer.PackNumber(sbyteValue);
                    break;
                case BlobValue blobValue:
                    Pack(blobValue, packer);
                    break;
                case GeoJSONValue geoJSONValue:
                    Pack(geoJSONValue, packer);
                    break;
                case HLLValue hLLValue:
                    Pack(hLLValue, packer);
                    break;
                case ValueArray<T> valueArrayValue:
                    Pack(valueArrayValue, packer);
                    break;
                case ListValue listValue:
                    Pack(listValue, packer);
                    break;
                case MapValue mapValue:
                    Pack(mapValue, packer);
                    break;
                default:
                    break;
            }
        }

        static public void Pack(ByteSegmentValue value, Packer packer) => value.Pack(packer);

        static public void Pack(BlobValue value, Packer packer) => value.Pack(packer);

        static public void Pack(GeoJSONValue value, Packer packer) => value.Pack(packer);

        static public void Pack(HLLValue value, Packer packer) => value.Pack(packer);

        static public void Pack(ValueArray<T> value, Packer packer) => value.Pack(packer);

        static public void Pack(ListValue value, Packer packer) => value.Pack(packer);

        static public void Pack(MapValue value, Packer packer) => value.Pack(packer);

        /// <summary>
		/// Validate if value type can be used as a key.
		/// </summary>
		/// <exception cref="AerospikeException">if type can't be used as a key.</exception>
        public void ValidateKeyType()
        {
            if (this.IsNull) throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: null");
            switch (this.value)
            {
                case bool boolValue:
                    throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: bool");
                case int iValue:
                    throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: BoolIntValue");
                case byte[] bValue:
                    throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: csblob");
                case BlobValue blobValue:
                    ValidateKeyType(blobValue);
                    break;
                case GeoJSONValue geoJSONValue:
                    ValidateKeyType(geoJSONValue);
                    break;
                case HLLValue hLLValue:
                    ValidateKeyType(hLLValue);
                    break;
                case ValueArray<T> valueArrayValue:
                    ValidateKeyType(valueArrayValue);
                    break;
                case ListValue listValue:
                    ValidateKeyType(listValue);
                    break;
                case MapValue mapValue:
                    ValidateKeyType(mapValue);
                    break;
                default:
                    break;
            }
        }

        static public void ValidateKeyType(BlobValue value) => value.ValidateKeyType();

        static public void ValidateKeyType(GeoJSONValue value) => value.ValidateKeyType();

        static public void ValidateKeyType(HLLValue value) => value.ValidateKeyType();

        static public void ValidateKeyType(ValueArray<T> value) => value.ValidateKeyType();

        static public void ValidateKeyType(ListValue value) => value.ValidateKeyType();

        static public void ValidateKeyType(MapValue value) => value.ValidateKeyType();

        public override bool Equals(object other)
		{
			if (other is Value<T> oValue) return this.Equals(oValue);
			if (this.IsNull && ReferenceEquals(other, null)) return true;

			return false;
		}

		public bool Equals(Value<T> other)
		{
			if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;

			return this.value.Equals(other.value);
		}

		public bool Equals(Value<byte[]> other)
		{
			if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
			if (this is Value<byte[]> oValue) return Util.ByteArrayEquals(oValue.value, other.value);

			return false;
		}

        public bool Equals(Value<ByteSegmentValue> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
            if (typeof(T) == typeof(ByteSegmentValue)) return this.value.Equals(other);

            return false;
        }

        public bool Equals(Value<BlobValue> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
            if (typeof(T) == typeof(BlobValue)) return this.value.Equals(other);

            return false;
        }

        public bool Equals(Value<GeoJSONValue> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
            if (typeof(T) == typeof(GeoJSONValue)) return this.value.Equals(other);

            return false;
        }

        public bool Equals(Value<HLLValue> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
            if (typeof(T) == typeof(HLLValue)) return this.value.Equals(other);

            return false;
        }

        public bool Equals(Value<ValueArray<T>> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
            if (typeof(T) == typeof(ValueArray<T>)) return this.value.Equals(other);

            return false;
        }

        public bool Equals(Value<ListValue> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
            if (typeof(T) == typeof(ListValue)) return this.value.Equals(other);

            return false;
        }

        public bool Equals(Value<MapValue> other)
        {
            if (this.IsNull || other.IsNull) return this.IsNull && other.IsNull;
            if (typeof(T) == typeof(MapValue)) return this.value.Equals(other);

            return false;
        }

        public override int GetHashCode()
        {
            if (this.IsNull) return 0;
            if (this is Value<byte[]> oItem) return this.GetHashCode(oItem);
            if (this is Value<ByteSegmentValue> oByteSegItem) return this.GetHashCode(oByteSegItem);
            if (this is Value<double> oDoubleItem) return this.GetHashCode(oDoubleItem);
            if (this is Value<float> oFloatItem) return this.GetHashCode(oFloatItem);
            if (this is Value<long> oLongItem) return this.GetHashCode(oLongItem);
            if (this is Value<ulong> uLongItem) return this.GetHashCode(uLongItem);
            if (this is Value<int> iItem) return iItem.value;
            if (this is Value<uint> uiItem) return (int)uiItem.value;
            if (this is Value<short> shItem) return (int)shItem.value;
            if (this is Value<ushort> ushItem) return (int)ushItem.value;
            if (this is Value<bool> boolItem) return boolItem.value ? 1231 : 1237;
            if (this is Value<byte> byteItem) return (int)byteItem.value;
            if (this is Value<sbyte> sbyteItem) return (int)sbyteItem.value;
            if (this is Value<BlobValue> blobItem) return this.GetHashCode(blobItem);
            if (this is Value<GeoJSONValue> geoJSONItem) return this.GetHashCode(geoJSONItem);
            if (this is Value<HLLValue> hllItem) return this.GetHashCode(hllItem);
            if (this is Value<ValueArray<T>> valueArrayItem) return this.GetHashCode(valueArrayItem);
            if (this is Value<ListValue> listValue) return this.GetHashCode(listValue);
            if (this is Value<MapValue> mapValue) return this.GetHashCode(mapValue);

            return this.value.GetHashCode();
        }

        public int GetHashCode(Value<byte[]> other) 
		{
			int result = 1;
			foreach(byte b in other.value)
			{
				result = 31 * result + b;
			}
			return result;
		}

        public int GetHashCode(Value<ByteSegmentValue> other) => other.value.GetHashCode();

        public int GetHashCode(Value<double> other)
        {
            ulong bits = (ulong)BitConverter.DoubleToInt64Bits(other.value);
            return (int)(bits ^ (bits >> 32));
        }

        public int GetHashCode(Value<float> other)
        {
            ulong bits = (ulong)BitConverter.DoubleToInt64Bits(other.value);
            return (int)(bits ^ (bits >> 32));
        }

        public int GetHashCode(Value<long> other)
        {
            return (int)((ulong)other.value ^ ((ulong)other.value >> 32));
        }

        public int GetHashCode(Value<ulong> other)
        {
            return (int)(other.value ^ (other.value >> 32));
        }

        public int GetHashCode(Value<BlobValue> other) => other.value.GetHashCode();

        public int GetHashCode(Value<GeoJSONValue> other) => other.value.GetHashCode();

        public int GetHashCode(Value<HLLValue> other) => other.value.GetHashCode();

        public int GetHashCode(Value<ValueArray<T>> other) => other.value.GetHashCode();

        public int GetHashCode(Value<ListValue> other) => other.value.GetHashCode();

        public int GetHashCode(Value<MapValue> other) => other.value.GetHashCode();

    }

    /// <summary>
    /// Empty value.
    /// </summary>
    public struct NullValue
    {
        public int Type { get => ParticleType.NULL; }
    }

    /// <summary>
    /// Byte segment value.
    /// </summary>
    public struct ByteSegmentValue : IEquatable<ByteSegmentValue>
    {
        public byte[] Bytes { get; }
        public int Offset { get; }
        public int Length { get; }

        public int Type
        {
            get
            {
                return ParticleType.BLOB;
            }
        }

        public ByteSegmentValue(byte[] bytes, int offset, int length)
        {
            this.Bytes = bytes;
            this.Offset = offset;
            this.Length = length;
        }

        public int EstimateSize()
        {
            return Length;
        }

        public int Write(byte[] buffer, int targetOffset)
        {
            Array.Copy(Bytes, Offset, buffer, targetOffset, Length);
            return Length;
        }

        public void Pack(Packer packer)
        {
            packer.PackParticleBytes(Bytes, Offset, Length);
        }

        public override string ToString()
        {
            return ByteUtil.BytesToHexString(Bytes, Offset, Length);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(ByteSegmentValue) == obj.GetType())
            {
                return this.Equals((ByteSegmentValue)obj);
            }

            return false;
        }

        public bool Equals(ByteSegmentValue other)
        {
            if (this.Length != other.Length)
            {
                return false;
            }

            for (int i = 0; i < Length; i++)
            {
                if (this.Bytes[this.Offset + i] != other.Bytes[other.Offset + i])
                {
                    return false;
                }
            }
            return true;
        }

        public override int GetHashCode()
        {
            int result = 1;
            for (int i = 0; i < Length; i++)
            {
                result = 31 * result + Bytes[Offset + i];
            }
            return result;
        }
    }

    public struct BlobValue : IEquatable<BlobValue>
    {
        public object Obj { get; }

        public byte[] Bytes { get; set; }

        public int Type
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

        // TODO: Ask richard about this
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
    public struct GeoJSONValue : IEquatable<GeoJSONValue>
    {
        public string value { get; }

        public int Type
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
    public struct HLLValue : IEquatable<HLLValue>
    {
        public byte[] Bytes { get; }

        public int Type
        {
            get { return ParticleType.HLL; }
        }

        public HLLValue(byte[] bytes)
        {
            this.Bytes = bytes;
        }

        public int EstimateSize()
        {
            return Bytes.Length;
        }

        public int Write(byte[] buffer, int offset)
        {
            Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
            return Bytes.Length;
        }

        public void Pack(Packer packer)
        {
            packer.PackParticleBytes(Bytes);
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: HLL");
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
    public struct ValueArray<T> : IEquatable<ValueArray<T>>
    {
        public Value<T>[] Array { get; set; }
        public byte[] Bytes { get; set; }

        public int Type
        {
            get
            {
                return ParticleType.LIST;
            }
        }

        public ValueArray(Value<T>[] array)
        {
            this.Array = array;
            this.Bytes = default(byte[]);
        }

        public int EstimateSize()
        {
            Bytes = Packer.Pack(Array);
            return Bytes.Length;
        }

        public int Write(byte[] buffer, int offset)
        {
            System.Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
            return Bytes.Length;
        }

        public void Pack(Packer packer)
        {
            packer.PackValueArray(Array);
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: value[]");
        }       

        public override string ToString()
        {
            return Util.ArrayToString(Array);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(ValueArray<T>) == obj.GetType())
            {
                return this.Equals((ValueArray<T>)obj);
            }

            return false;
        }

        public bool Equals(ValueArray<T> other)
        {
            if (this.Array.Length != other.Array.Length)
            {
                return false;
            }

            for (int i = 0; i < this.Array.Length; i++)
            {
                Value<T> v1 = this.Array[i];
                Value<T> v2 = other.Array[i];

                if (v1.value == null)
                {
                    if (v2.value == null)
                    {
                        continue;
                    }
                    return false;
                }

                if (!v1.value.Equals(v2.value))
                {
                    return false;
                }
            }
            return true;
        }

        public override int GetHashCode()
        {
            int result = 1;
            foreach (Value<T> item in Array)
            {
                result = 31 * result + (item.value == null ? 0 : item.value.GetHashCode());
            }
            return result;
        }
    }

    /// <summary>
    /// List value.
    /// </summary>
    public struct ListValue : IEquatable<ListValue>
    {
        internal readonly IList List;
        internal byte[] Bytes;

        public int Type
        {
            get
            {
                return ParticleType.LIST;
            }
        }

        public ListValue(IList list)
        {
            this.List = list;
            this.Bytes = default(byte[]);
        }

        public int EstimateSize()
        {
            Bytes = Packer.Pack(List);
            return Bytes.Length;
        }

        public int Write(byte[] buffer, int offset)
        {
            Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
            return Bytes.Length;
        }

        public void Pack(Packer packer)
        {
            packer.PackList(List);
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: list");
        }

        public override string ToString()
        {
            return List.ToString();
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
            if (this.List.Count != other.List.Count)
            {
                return false;
            }

            for (int i = 0; i < this.List.Count; i++)
            {
                object v1 = this.List[i];
                object v2 = other.List[i];

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
            foreach (object value in List)
            {
                result = 31 * result + (value == null ? 0 : value.GetHashCode());
            }
            return result;
        }
    }

    /// <summary>
    /// Map value.
    /// </summary>
    public struct MapValue : IEquatable<MapValue>
    {
        internal readonly IDictionary Map;
        internal readonly MapOrder Order;
        internal byte[] Bytes;

        public int Type
        {
            get
            {
                return ParticleType.MAP;
            }
        }

        public MapValue(IDictionary map)
        {
            this.Map = map;
            this.Order = MapOrder.UNORDERED;
            this.Bytes = default(byte[]);
        }

        public MapValue(IDictionary map, MapOrder order)
        {
            this.Map = map;
            this.Order = order;
            this.Bytes = default(byte[]);
        }

        public MapOrder MapOrder
        {
            get { return MapOrder; }
        }

        public int EstimateSize()
        {
            Bytes = Packer.Pack(Map, Order);
            return Bytes.Length;
        }

        public int Write(byte[] buffer, int offset)
        {
            Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
            return Bytes.Length;
        }

        public void Pack(Packer packer)
        {
            packer.PackMap(Map, Order);
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: map");
        }

        public override string ToString()
        {
            return Map.ToString();
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
            if (this.Map.Count != other.Map.Count)
            {
                return false;
            }

            try
            {
                foreach (DictionaryEntry entry in this.Map)
                {
                    object v1 = entry.Value;
                    object v2 = other.Map[entry.Key];

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
            foreach (DictionaryEntry entry in Map)
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
    public struct InfinityValue
    {
        public int Type
        {
            get
            {
                throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid particle type: INF");
            }
        }

        public int EstimateSize()
        {
            return 0;
        }

        public int Write(byte[] buffer, int offset)
        {
            return 0;
        }

        public void Pack(Packer packer)
        {
            packer.PackInfinity();
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: INF");
        }

        public override string ToString()
        {
            return "INF";
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(InfinityValue) == obj.GetType())
            {
                return true;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return 0;
        }
    }

    /// <summary>
    /// Wildcard value.
    /// </summary>
    public struct WildcardValue
    {
        public int Type
        {
            get
            {
                throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid particle type: wildcard");
            }
        }

        public int EstimateSize()
        {
            return 0;
        }

        public int Write(byte[] buffer, int offset)
        {
            return 0;
        }

        public void Pack(Packer packer)
        {
            packer.PackWildcard();
        }

        public void ValidateKeyType()
        {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: wildcard");
        }

        public override string ToString()
        {
            return "*";
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (typeof(WildcardValue) == obj.GetType())
            {
                return true;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return 0;
        }
    }
}
