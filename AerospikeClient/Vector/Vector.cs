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
using System.Buffers.Binary;

namespace Aerospike.Client
{
	/// <summary>
	/// Vector of numeric elements, used for vector similarity search. A vector is
	/// defined by the following wire format:
	/// <para>
	/// Offset  Size (bytes)  Field         Description
	/// 0       1             version       The version of the vector format.
	/// 1       1             element_type  Enum identifying the element type.
	/// 2       4             dimensions    Number of dimensions.
	/// 6       variable      data          Contiguous little-endian element array.
	/// </para>
	/// Element payloads are little-endian (host order on typical servers), not the
	/// big-endian convention used by scalar Aerospike particles.
	/// </summary>
	public sealed class Vector : IEquatable<Vector>
	{
		/// <summary>
		/// Current vector format version.
		/// </summary>
		public const byte VERSION = 1;

		private const int VERSION_SIZE = 1;
		private const int ELEMENT_TYPE_SIZE = 1;
		private const int DIMENSIONS_SIZE = 4;

		/// <summary>
		/// Size in bytes of the fixed header (version + element_type + dimensions).
		/// </summary>
		public const int HEADER_SIZE = VERSION_SIZE + ELEMENT_TYPE_SIZE + DIMENSIONS_SIZE;

		/// <summary>
		/// Vector element type. Values are mutually exclusive wire codes (not a bitfield).
		/// </summary>
		public enum ElementType : byte
		{
			/// <summary>
			/// float16: high-density vectors (IEEE 754 half).
			/// </summary>
			FLOAT16 = 0x01,

			/// <summary>
			/// int32: integer-based embeddings.
			/// </summary>
			INT32 = 0x02,

			/// <summary>
			/// float (fp32): standard FP32.
			/// </summary>
			FLOAT32 = 0x03,

			/// <summary>
			/// double (fp64): high-precision FP64.
			/// </summary>
			FLOAT64 = 0x04
		}

		/// <summary>
		/// Return the number of bytes used to encode a single element of the given type.
		/// </summary>
		public static int GetElementByteSize(ElementType elementType) => elementType switch
		{
			ElementType.FLOAT16 => 2,
			ElementType.INT32 => 4,
			ElementType.FLOAT32 => 4,
			ElementType.FLOAT64 => 8,
			_ => throw new ArgumentException("Unknown vector element type: " + elementType)
		};

		/// <summary>
		/// Lookup element type from its wire protocol code.
		/// </summary>
		public static ElementType ElementTypeFromCode(byte code) => code switch
		{
			(byte)ElementType.FLOAT16 => ElementType.FLOAT16,
			(byte)ElementType.INT32 => ElementType.INT32,
			(byte)ElementType.FLOAT32 => ElementType.FLOAT32,
			(byte)ElementType.FLOAT64 => ElementType.FLOAT64,
			_ => throw new ArgumentException("Unknown vector element type code: " + code)
		};

		/// <summary>
		/// Vector format version.
		/// </summary>
		public readonly byte version;

		/// <summary>
		/// Vector element type.
		/// </summary>
		public readonly ElementType elementType;

		/// <summary>
		/// Number of dimensions (elements) in this vector.
		/// </summary>
		public readonly int dimensions;

		private readonly object data;

		private int? wireSize;
		private int? hash;

		private Vector(byte version, ElementType elementType, int dimensions, object data)
		{
			this.version = version;
			this.elementType = elementType;
			this.dimensions = dimensions;
			this.data = data;
		}

		/// <summary>
		/// Return the number of bytes needed to serialize this vector in wire format
		/// (header plus element data). Computed lazily and cached on first call.
		/// </summary>
		public int GetWireSize()
		{
			wireSize ??= HEADER_SIZE + (dimensions * GetElementByteSize(elementType));
			return wireSize.Value;
		}

		/// <summary>
		/// Serialize this vector into the wire format at the given buffer offset.
		/// Operates directly on the internal data array (no defensive copy) since
		/// this is used on the record write hot path.
		/// </summary>
		/// <returns>number of bytes written, equal to <see cref="GetWireSize"/></returns>
		public int WriteTo(byte[] buffer, int offset)
		{
			int pos = offset;

			// Vector wire format is little-endian to match the server.
			buffer[pos++] = version;
			buffer[pos++] = (byte)elementType;
			ByteUtil.IntToLittleBytes((uint)dimensions, buffer, pos);
			pos += DIMENSIONS_SIZE;

			int dataSize = dimensions * GetElementByteSize(elementType);
			WriteElements(buffer.AsSpan(pos, dataSize));
			return pos + dataSize - offset;
		}

		/// <summary>
		/// Return the complete vector wire value, including the 6-byte header.
		/// </summary>
		public byte[] GetWireBytes()
		{
			byte[] bytes = new byte[GetWireSize()];
			WriteTo(bytes, 0);
			return bytes;
		}

		/// <summary>
		/// Return the raw element array in little-endian wire byte order, without the
		/// 6-byte header.
		/// </summary>
		public byte[] GetElementBytes()
		{
			byte[] bytes = new byte[dimensions * GetElementByteSize(elementType)];
			WriteElements(bytes);
			return bytes;
		}

		/// <summary>
		/// Deserialize a vector from wire format at the given buffer offset.
		/// </summary>
		/// <param name="buffer">buffer containing the vector wire format</param>
		/// <param name="offset">offset in buffer where the vector starts</param>
		/// <param name="length">number of bytes available for this vector (must be at least HEADER_SIZE)</param>
		public static Vector From(byte[] buffer, int offset, int length)
		{
			if (length < HEADER_SIZE)
			{
				throw new ArgumentException("Invalid vector length: " + length);
			}

			int pos = offset;

			byte version = buffer[pos++];
			ElementType elementType = ElementTypeFromCode(buffer[pos++]);
			int dimensions = ByteUtil.LittleBytesToInt(buffer, pos);
			pos += DIMENSIONS_SIZE;

			// Permissive decode: allow dims == 0 (server rejects zero-dim writes).
			// Negative dims indicate a corrupt header.
			if (dimensions < 0)
			{
				throw new ArgumentException("Invalid vector dimensions: " + dimensions);
			}

			// Use long math so a large dimensions count cannot overflow the int size computation
			// (which could otherwise bypass the bounds check and trigger a huge allocation).
			long dataSizeLong = (long)dimensions * GetElementByteSize(elementType);

			if (length < HEADER_SIZE + dataSizeLong)
			{
				throw new ArgumentException("Invalid vector length: " + length +
					", expected at least " + (HEADER_SIZE + dataSizeLong));
			}

			// Safe to narrow: dataSizeLong <= length - HEADER_SIZE, and length is an int.
			int dataSize = (int)dataSizeLong;
			object data = ReadElements(elementType, dimensions, buffer.AsSpan(pos, dataSize));

			return new Vector(version, elementType, dimensions, data);
		}

		/// <summary>
		/// Create a vector of raw float16 (IEEE 754 half precision) elements.
		/// Since C# has no native float16 type here, each element is passed as its
		/// raw 16-bit bit pattern.
		/// </summary>
		public static Vector OfFloat16(short[] data) =>
			new(VERSION, ElementType.FLOAT16, RequireNonNull(data, nameof(data)).Length, (short[])data.Clone());

		/// <summary>
		/// Create a vector of int32 elements.
		/// </summary>
		public static Vector OfInt32(int[] data) =>
			new(VERSION, ElementType.INT32, RequireNonNull(data, nameof(data)).Length, (int[])data.Clone());

		/// <summary>
		/// Create a vector of float (fp32) elements.
		/// </summary>
		public static Vector OfFloat32(float[] data) =>
			new(VERSION, ElementType.FLOAT32, RequireNonNull(data, nameof(data)).Length, (float[])data.Clone());

		/// <summary>
		/// Create a vector of double (fp64) elements.
		/// </summary>
		public static Vector OfFloat64(double[] data) =>
			new(VERSION, ElementType.FLOAT64, RequireNonNull(data, nameof(data)).Length, (double[])data.Clone());

		/// <summary>
		/// Empty arrays are allowed locally; the server rejects zero-dimension writes
		/// with PARAMETER_ERROR (cross-client rule matching Java).
		/// </summary>
		private static T[] RequireNonNull<T>(T[] data, string paramName)
		{
			if (data is null)
			{
				throw new ArgumentNullException(paramName);
			}
			return data;
		}

		/// <summary>
		/// Return raw float16 data. Throws <see cref="InvalidOperationException"/> if this
		/// vector's element type is not <see cref="ElementType.FLOAT16"/>.
		/// </summary>
		public short[] GetFloat16Data()
		{
			ValidateType(ElementType.FLOAT16);
			return (short[])((short[])data).Clone();
		}

		/// <summary>
		/// Return int32 data. Throws <see cref="InvalidOperationException"/> if this
		/// vector's element type is not <see cref="ElementType.INT32"/>.
		/// </summary>
		public int[] GetInt32Data()
		{
			ValidateType(ElementType.INT32);
			return (int[])((int[])data).Clone();
		}

		/// <summary>
		/// Return float (fp32) data. Throws <see cref="InvalidOperationException"/> if this
		/// vector's element type is not <see cref="ElementType.FLOAT32"/>.
		/// </summary>
		public float[] GetFloat32Data()
		{
			ValidateType(ElementType.FLOAT32);
			return (float[])((float[])data).Clone();
		}

		/// <summary>
		/// Return double (fp64) data. Throws <see cref="InvalidOperationException"/> if this
		/// vector's element type is not <see cref="ElementType.FLOAT64"/>.
		/// </summary>
		public double[] GetFloat64Data()
		{
			ValidateType(ElementType.FLOAT64);
			return (double[])((double[])data).Clone();
		}

		private void ValidateType(ElementType expected)
		{
			if (elementType != expected)
			{
				throw new InvalidOperationException(
					"Vector element type is " + elementType + ", not " + expected);
			}
		}

		public override string ToString() => elementType switch
		{
			ElementType.FLOAT16 => "[" + string.Join(", ", (short[])data) + "]",
			ElementType.INT32 => "[" + string.Join(", ", (int[])data) + "]",
			ElementType.FLOAT32 => "[" + string.Join(", ", (float[])data) + "]",
			ElementType.FLOAT64 => "[" + string.Join(", ", (double[])data) + "]",
			_ => data.ToString()
		};

		public override bool Equals(object obj) => Equals(obj as Vector);

		public bool Equals(Vector other)
		{
			if (other is null)
			{
				return false;
			}

			if (ReferenceEquals(this, other))
			{
				return true;
			}

			if (version != other.version || elementType != other.elementType || dimensions != other.dimensions)
			{
				return false;
			}

			// Match Java Arrays.equals(float[]/double[]): bit semantics so NaN==NaN
			// and -0.0 != +0.0.
			return elementType switch
			{
				ElementType.FLOAT16 => ((short[])data).AsSpan().SequenceEqual((short[])other.data),
				ElementType.INT32 => ((int[])data).AsSpan().SequenceEqual((int[])other.data),
				ElementType.FLOAT32 => FloatBitsEqual((float[])data, (float[])other.data),
				ElementType.FLOAT64 => DoubleBitsEqual((double[])data, (double[])other.data),
				_ => data.Equals(other.data)
			};
		}

		public override int GetHashCode()
		{
			hash ??= ComputeHashCode();
			return hash.Value;
		}

		private int ComputeHashCode()
		{
			int h = version;
			h = 31 * h + elementType.GetHashCode();
			h = 31 * h + dimensions;
			h = 31 * h + (elementType switch
			{
				ElementType.FLOAT16 => HashCode((short[])data),
				ElementType.INT32 => HashCode((int[])data),
				ElementType.FLOAT32 => HashCode((float[])data),
				ElementType.FLOAT64 => HashCode((double[])data),
				_ => data.GetHashCode()
			});
			return h;
		}

		public static bool operator ==(Vector o1, Vector o2) => o1?.Equals(o2) ?? o2 is null;
		public static bool operator !=(Vector o1, Vector o2) => !(o1 == o2);

		private void WriteElements(Span<byte> view)
		{
			switch (elementType)
			{
				case ElementType.FLOAT16:
					WriteFloat16(view, (short[])data);
					break;
				case ElementType.INT32:
					WriteInt32(view, (int[])data);
					break;
				case ElementType.FLOAT32:
					WriteFloat32(view, (float[])data);
					break;
				case ElementType.FLOAT64:
					WriteFloat64(view, (double[])data);
					break;
				default:
					throw new InvalidOperationException("Unsupported vector element type: " + elementType);
			}
		}

		private static object ReadElements(ElementType elementType, int dimensions, ReadOnlySpan<byte> view) =>
			elementType switch
			{
				ElementType.FLOAT16 => ReadFloat16(view, dimensions),
				ElementType.INT32 => ReadInt32(view, dimensions),
				ElementType.FLOAT32 => ReadFloat32(view, dimensions),
				ElementType.FLOAT64 => ReadFloat64(view, dimensions),
				_ => throw new InvalidOperationException("Unsupported vector element type: " + elementType)
			};

		private static int HashCode(short[] data)
		{
			int h = 1;
			foreach (short v in data)
			{
				h = 31 * h + v;
			}
			return h;
		}

		private static int HashCode(int[] data)
		{
			int h = 1;
			foreach (int v in data)
			{
				h = 31 * h + v;
			}
			return h;
		}

		private static int HashCode(float[] data)
		{
			int h = 1;
			foreach (float v in data)
			{
				h = 31 * h + BitConverter.SingleToInt32Bits(v);
			}
			return h;
		}

		private static int HashCode(double[] data)
		{
			int h = 1;
			foreach (double v in data)
			{
				long bits = BitConverter.DoubleToInt64Bits(v);
				h = 31 * h + (int)(bits ^ (bits >> 32));
			}
			return h;
		}

		private static bool FloatBitsEqual(float[] a, float[] b)
		{
			if (a.Length != b.Length)
			{
				return false;
			}

			for (int i = 0; i < a.Length; i++)
			{
				if (BitConverter.SingleToInt32Bits(a[i]) != BitConverter.SingleToInt32Bits(b[i]))
				{
					return false;
				}
			}
			return true;
		}

		private static bool DoubleBitsEqual(double[] a, double[] b)
		{
			if (a.Length != b.Length)
			{
				return false;
			}

			for (int i = 0; i < a.Length; i++)
			{
				if (BitConverter.DoubleToInt64Bits(a[i]) != BitConverter.DoubleToInt64Bits(b[i]))
				{
					return false;
				}
			}
			return true;
		}

		private static void WriteFloat16(Span<byte> view, short[] data)
		{
			for (int i = 0; i < data.Length; i++)
			{
				BinaryPrimitives.WriteInt16LittleEndian(view.Slice(i * 2, 2), data[i]);
			}
		}

		private static void WriteInt32(Span<byte> view, int[] data)
		{
			for (int i = 0; i < data.Length; i++)
			{
				BinaryPrimitives.WriteInt32LittleEndian(view.Slice(i * 4, 4), data[i]);
			}
		}

		private static void WriteFloat32(Span<byte> view, float[] data)
		{
			for (int i = 0; i < data.Length; i++)
			{
				BinaryPrimitives.WriteSingleLittleEndian(view.Slice(i * 4, 4), data[i]);
			}
		}

		private static void WriteFloat64(Span<byte> view, double[] data)
		{
			for (int i = 0; i < data.Length; i++)
			{
				BinaryPrimitives.WriteDoubleLittleEndian(view.Slice(i * 8, 8), data[i]);
			}
		}

		private static short[] ReadFloat16(ReadOnlySpan<byte> view, int dimensions)
		{
			short[] data = new short[dimensions];
			for (int i = 0; i < dimensions; i++)
			{
				data[i] = BinaryPrimitives.ReadInt16LittleEndian(view.Slice(i * 2, 2));
			}
			return data;
		}

		private static int[] ReadInt32(ReadOnlySpan<byte> view, int dimensions)
		{
			int[] data = new int[dimensions];
			for (int i = 0; i < dimensions; i++)
			{
				data[i] = BinaryPrimitives.ReadInt32LittleEndian(view.Slice(i * 4, 4));
			}
			return data;
		}

		private static float[] ReadFloat32(ReadOnlySpan<byte> view, int dimensions)
		{
			float[] data = new float[dimensions];
			for (int i = 0; i < dimensions; i++)
			{
				data[i] = BinaryPrimitives.ReadSingleLittleEndian(view.Slice(i * 4, 4));
			}
			return data;
		}

		private static double[] ReadFloat64(ReadOnlySpan<byte> view, int dimensions)
		{
			double[] data = new double[dimensions];
			for (int i = 0; i < dimensions; i++)
			{
				data[i] = BinaryPrimitives.ReadDoubleLittleEndian(view.Slice(i * 8, 8));
			}
			return data;
		}
	}
}
