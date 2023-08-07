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
		/// is used. BinaryFormatter serialization is triggered when a bin constructed by
		/// <see cref="Bin.Bin(string, object)"/> or <see cref="Bin.AsBlob(string, object)"/> 
		/// is used in a write command with an unrecognized object type.
		/// </summary>
		public static bool DisableSerializer = false;

		/// <summary>
		/// Should default object deserializer be disabled. If true, an exception will be thrown when
		/// a default object deserialization is attempted. Default object serialization is triggered
		/// when serialized data is read/parsed from the server. DisableDeserializer is separate from
		/// DisableSerializer because there may be cases when no new serialization is allowed, but
		/// existing serialized objects need to be supported.
		/// </summary>
		public static bool DisableDeserializer = false;

		/// <summary>
		/// Null value.
		/// </summary>
		public static readonly Value NULL = NullValue.Instance;

		/// <summary>
		/// Infinity value to be used in CDT range comparisons only.
		/// </summary>
		public static readonly Value INFINITY = new InfinityValue();

		/// <summary>
		/// Wildcard value to be used in CDT range comparisons only.
		/// </summary>
		public static readonly Value WILDCARD = new WildcardValue();
	
		/// <summary>
		/// Get string or null value instance.
		/// </summary>
		public static Value Get(string value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new StringValue(value);
			}
		}

		/// <summary>
		/// Get byte array or null value instance.
		/// </summary>
		public static Value Get(byte[] value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new BytesValue(value);
			}
		}

		/// <summary>
		/// Get byte array segment or null value instance.
		/// </summary>
		public static Value Get(byte[] value, int offset, int length)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new ByteSegmentValue(value, offset, length);
			}
		}

		/// <summary>
		/// Get double value instance.
		/// </summary>
		public static Value Get(double value)
		{
			return new DoubleValue(value);
		}

		/// <summary>
		/// Get float value instance.
		/// </summary>
		public static Value Get(float value)
		{
			return new FloatValue(value);
		}

		/// <summary>
		/// Get long value instance.
		/// </summary>
		public static Value Get(long value)
		{
			return new LongValue(value);
		}

		/// <summary>
		/// Get unsigned long value instance.
		/// </summary>
		public static Value Get(ulong value)
		{
			return new UnsignedLongValue(value);
		}

		/// <summary>
		/// Get integer value instance.
		/// </summary>
		public static Value Get(int value)
		{
			return new IntegerValue(value);
		}

		/// <summary>
		/// Get unsigned integer value instance.
		/// </summary>
		public static Value Get(uint value)
		{
			return new UnsignedIntegerValue(value);
		}

		/// <summary>
		/// Get short value instance.
		/// </summary>
		public static Value Get(short value)
		{
			return new ShortValue(value);
		}

		/// <summary>
		/// Get short value instance.
		/// </summary>
		public static Value Get(ushort value)
		{
			return new UnsignedShortValue(value);
		}

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
		public static Value Get(byte value)
		{
			return new ByteValue(value);
		}

		/// <summary>
		/// Get signed boolean value instance.
		/// </summary>
		public static Value Get(sbyte value)
		{
			return new SignedByteValue(value);
		}

		/// <summary>
		/// Get list or null value instance.
		/// </summary>
		public static Value Get(IList value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new ListValue(value);
			}
		}

		/// <summary>
		/// Get map or null value instance.
		/// </summary>
		public static Value Get(IDictionary value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new MapValue(value);
			}
		}

		/// <summary>
		/// Get map or null value instance.
		/// </summary>
		public static Value Get(IDictionary value, MapOrder order)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new MapValue(value, order);
			}
		}

		/// <summary>
		/// Get value array instance.
		/// </summary>
		public static Value Get(Value[] value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new ValueArray(value);
			}
		}

		/// <summary>
		/// Get blob or null value instance.
		/// </summary>
		public static Value GetAsBlob(object value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new BlobValue(value);
			}
		}

		/// <summary>
		/// Get GeoJSON or null value instance.
		/// </summary>
		public static Value GetAsGeoJSON(string value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new GeoJSONValue(value);
			}
		}

		/// <summary>
		/// Get HyperLogLog or null value instance.
		/// </summary>
		public static Value GetAsHLL(byte[] value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}
			else
			{
				return new HLLValue(value);
			}
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
		/// Determine value given generic object.
		/// This is the slowest of the Value get() methods.
		/// Useful when copying records from one cluster to another.
		/// </summary>
		public static Value Get(object value)
		{
			if (value == null)
			{
				return NullValue.Instance;
			}

			if (value is byte[])
			{
				return new BytesValue((byte[])value);
			}

			if (value is Value)
			{
				return (Value)value;
			}

			if (value is IList)
			{
				return new ListValue((IList)value);
			}

			if (value is IDictionary)
			{
				return new MapValue((IDictionary)value);
			}

			TypeCode code = System.Type.GetTypeCode(value.GetType());

			switch (code)
			{
                case TypeCode.Empty:
					return NullValue.Instance;

                case TypeCode.String:
					return new StringValue((string)value);

				case TypeCode.Double:
					return new DoubleValue((double)value);

				case TypeCode.Single:
					return new FloatValue((float)value);

				case TypeCode.Int64:
					return new LongValue((long)value);

				case TypeCode.Int32:
					return new IntegerValue((int)value);

				case TypeCode.Int16:
					return new ShortValue((short)value);

				case TypeCode.UInt64:
					return new UnsignedLongValue((ulong)value);

				case TypeCode.UInt32:
					return new UnsignedIntegerValue((uint)value);

				case TypeCode.UInt16:
					return new UnsignedShortValue((ushort)value);

				case TypeCode.Boolean:
					if (UseBoolBin)
					{
						return new BooleanValue((bool)value);
					}
					else
					{
						return new BoolIntValue((bool)value);
					}
 
                case TypeCode.Byte:
					return new ByteValue((byte)value);

                case TypeCode.SByte:
					return new SignedByteValue((sbyte)value);

				case TypeCode.Char:
                case TypeCode.DateTime:
				case TypeCode.Decimal:
                default:
					return new BlobValue(value);
			}
		}

		/// <summary>
		/// Get value from Record object. Useful when copying records from one cluster to another.
		/// </summary>
		public static Value GetFromRecordObject(object value)
		{
			return Value.Get(value);
		}

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
		/// Get wire protocol value type.
		/// </summary>
		public abstract int Type {get;}

		/// <summary>
		/// Return original value as an Object.
		/// </summary>
		public abstract object Object {get;}

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
			
			public override int Type
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

			public override bool Equals(object other)
			{
				if (other == null)
				{
					return true;
				}
				return this.GetType().Equals(other.GetType());
			}

			public override int GetHashCode()
			{
				return 0;
			}
		}

		/// <summary>
		/// String value.
		/// </summary>
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null && 
					this.GetType().Equals(other.GetType()) && 
					this.value.Equals(((StringValue)other).value));
			}

			public override int GetHashCode()
			{
				return value.GetHashCode();
			}
		}

		/// <summary>
		/// Byte array value.
		/// </summary>
		public sealed class BytesValue : Value
		{
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					Util.ByteArrayEquals(this.bytes, ((BytesValue)other).bytes));
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
		public sealed class ByteSegmentValue : Value
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

			public override void Pack(Packer packer)
			{
				packer.PackParticleBytes(bytes, offset, length);
			}

			public override int Type
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
					return this;
				}
			}

			public override string ToString()
			{
				return ByteUtil.BytesToHexString(bytes, offset, length);
			}

			public override bool Equals(object obj)
			{
				if (obj == null)
				{
					return false;
				}

				if (!this.GetType().Equals(obj.GetType()))
				{
					return false;
				}
				ByteSegmentValue other = (ByteSegmentValue)obj;

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
		public sealed class DoubleValue : Value
		{
			private readonly double value;

			public DoubleValue(double value)
			{
				this.value = value;
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
				packer.PackDouble(value);
			}

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) && 
					this.value == ((DoubleValue)other).value);
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
		public sealed class FloatValue : Value
		{
			private readonly float value;

			public FloatValue(float value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((FloatValue)other).value);
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
		public sealed class LongValue : Value
		{
			private readonly long value;

			public LongValue(long value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((LongValue)other).value);
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
		public sealed class UnsignedLongValue : Value
		{
			private readonly ulong value;

			public UnsignedLongValue(ulong value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return ((value & 0x8000000000000000) == 0)? 8 : 9;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.LongToBytes(value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((UnsignedLongValue)other).value);
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
		public sealed class IntegerValue : Value
		{
			private readonly int value;

			public IntegerValue(int value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((IntegerValue)other).value);
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
		public sealed class UnsignedIntegerValue : Value
		{
			private readonly uint value;

			public UnsignedIntegerValue(uint value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((UnsignedIntegerValue)other).value);
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
		public sealed class ShortValue : Value
		{
			private readonly short value;

			public ShortValue(short value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((ShortValue)other).value);
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
		public sealed class UnsignedShortValue : Value
		{
			private readonly ushort value;

			public UnsignedShortValue(ushort value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((UnsignedShortValue)other).value);
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
		public sealed class BooleanValue : Value
		{
			private readonly bool value;

			public BooleanValue(bool value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((BooleanValue)other).value);
			}

			public override int GetHashCode()
			{
				return value ? 1231 : 1237;
			}

			public override int ToInteger()
			{
				return value? 1 : 0;
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
		public sealed class BoolIntValue : Value
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((BoolIntValue)other).value);
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
		public sealed class ByteValue : Value
		{
			private readonly byte value;

			public ByteValue(byte value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((ByteValue)other).value);
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
		public sealed class SignedByteValue : Value
		{
			private readonly sbyte value;

			public SignedByteValue(sbyte value)
			{
				this.value = value;
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

			public override int Type
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value == ((SignedByteValue)other).value);
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
		public sealed class BlobValue : Value
		{
			private readonly object obj;
			private byte[] bytes;

			public BlobValue(object obj)
			{
				this.obj = obj;
			}

			public override int EstimateSize()
			{
				bytes = Serialize(obj);
				return bytes.Length;
			}

			public static byte[] Serialize(object val)
			{
#if BINARY_FORMATTER
			if (DisableSerializer) throw new AerospikeException("Object serializer has been disabled");

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

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(bytes, 0, buffer, offset, bytes.Length);
				return bytes.Length;
			}

			public override void Pack(Packer packer)
			{
				// Do not try to pack bytes field because it will be null
				// when packing objects in a collection (ie. EstimateSize() not called).
				packer.PackBlob(obj);
			}

			public override void ValidateKeyType()
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: csblob");
			}

			public override int Type
			{
				get
				{
					return ParticleType.CSHARP_BLOB;
				}
			}

			public override object Object
			{
				get
				{
					return obj;
				}
			}

			public override string ToString()
			{
				return ByteUtil.BytesToHexString(bytes);
			}

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.obj.Equals(((BlobValue)other).obj));
			}

			public override int GetHashCode()
			{
				return obj.GetHashCode();
			}
		}

		/// <summary>
		/// GeoJSON value.
		/// </summary>
		public sealed class GeoJSONValue : Value
		{
			private readonly string value;

			public GeoJSONValue(string value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				// flags + ncells + jsonstr
				return 1 + 2 + ByteUtil.EstimateSizeUtf8(value);
			}

			public override int Write(byte[] buffer, int offset)
			{
				buffer[offset] = 0; // flags
				ByteUtil.ShortToBytes(0, buffer, offset + 1); // ncells
				return 1 + 2 + ByteUtil.StringToUtf8(value, buffer, offset + 3); // jsonstr
			}

			public override void Pack(Packer packer)
			{
				packer.PackGeoJSON(value);
			}

			public override void ValidateKeyType()
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: GeoJSON");
			}

			public override int Type
			{
				get
				{
					return ParticleType.GEOJSON;
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

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					this.value.Equals(((GeoJSONValue)other).value));
			}

			public override int GetHashCode()
			{
				return value.GetHashCode();
			}
		}

		/// <summary>
		/// HyperLogLog value.
		/// </summary>
		public sealed class HLLValue : Value
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

			public override int Type
			{
				get{ return ParticleType.HLL; }
			}

			public override object Object
			{
				get{ return bytes; }
			}

			public byte[] Bytes
			{
				get { return bytes; }
			}

			public override string ToString()
			{
				return ByteUtil.BytesToHexString(bytes);
			}

			public override bool Equals(object other)
			{
				return (other != null &&
					this.GetType().Equals(other.GetType()) &&
					Util.ByteArrayEquals(this.bytes, ((HLLValue)other).bytes));
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
		/// Value array.
		/// </summary>
		public sealed class ValueArray : Value
		{
			private readonly Value[] array;
			private byte[] bytes;

			public ValueArray(Value[] array)
			{
				this.array = array;
			}

			public override int EstimateSize()
			{
				bytes = Packer.Pack(array);
				return bytes.Length;
			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(bytes, 0, buffer, offset, bytes.Length);
				return bytes.Length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackValueArray(array);
			}

			public override void ValidateKeyType()
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: value[]");
			}

			public override int Type
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
					return array;
				}
			}

			public override string ToString()
			{
				return Util.ArrayToString(array);
			}

			public override bool Equals(object obj)
			{
				if (obj == null)
				{
					return false;
				}

				if (!this.GetType().Equals(obj.GetType()))
				{
					return false;
				}
				ValueArray other = (ValueArray)obj;

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
				foreach (Value value in array)
				{
					result = 31 * result + (value == null ? 0 : value.GetHashCode());
				}
				return result;
			}
		}

		/// <summary>
		/// List value.
		/// </summary>
		public sealed class ListValue : Value
		{
			internal readonly IList list;
			internal byte[] bytes;

			public ListValue(IList list)
			{
				this.list = list;
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

			public override int Type
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

				if (!this.GetType().Equals(obj.GetType()))
				{
					return false;
				}
				ListValue other = (ListValue)obj;

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
		public sealed class MapValue : Value
		{
			internal readonly IDictionary map;
			internal readonly MapOrder order;
			internal byte[] bytes;

			public MapValue(IDictionary map)
			{
				this.map = map;
				this.order = MapOrder.UNORDERED;
			}

			public MapValue(IDictionary map, MapOrder order)
			{
				this.map = map;
				this.order = order;
			}

			public MapOrder Order
			{
				get { return order; }
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

			public override int Type
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

				if (!this.GetType().Equals(obj.GetType()))
				{
					return false;
				}
				MapValue other = (MapValue)obj;

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

			public override int Type
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

			public override int Type
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
}
