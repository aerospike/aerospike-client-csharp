/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
namespace Aerospike.Client
{
	/// <summary>
	/// Query filter used to narrow down query results.
	/// </summary>
	public sealed class Filter
	{
		/// <summary>
		/// Create long equality filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="value">filter value</param>
		public static Filter Equal(string name, long value)
		{
			Value val = Value.Get(value);
			return new Filter(name, val, val);
		}

		/// <summary>
		/// Create string equality filter for query.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="value">filter value</param>
		public static Filter Equal(string name, string value)
		{
			Value val = Value.Get(value);
			return new Filter(name, val, val);
		}

		/// <summary>
		/// Create range filter for query.
		/// Range arguments must be longs or integers which can be cast to longs.
		/// String ranges are not supported.
		/// </summary>
		/// <param name="name">bin name</param>
		/// <param name="begin">filter begin value</param>
		/// <param name="end">filter end value</param>
		public static Filter Range(string name, long begin, long end)
		{
			return new Filter(name, Value.Get(begin), Value.Get(end));
		}

		private readonly string name;
		private readonly Value begin;
		private readonly Value end;

		private Filter(string name, Value begin, Value end)
		{
			this.name = name;
			this.begin = begin;
			this.end = end;
		}

		internal int EstimateSize()
		{
			// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
			return ByteUtil.EstimateSizeUtf8(name) + begin.EstimateSize() + end.EstimateSize() + 10;
		}

		internal int Write(byte[] buf, int offset)
		{
			// Write name.
			int len = ByteUtil.StringToUtf8(name, buf, offset + 1);
			buf[offset] = (byte)len;
			offset += len + 1;

			// Write particle type.
			buf[offset++] = (byte)begin.Type;

			// Write filter begin.
			len = begin.Write(buf, offset + 4);
			ByteUtil.IntToBytes((uint)len, buf, offset);
			offset += len + 4;

			// Write filter end.
			len = end.Write(buf, offset + 4);
			ByteUtil.IntToBytes((uint)len, buf, offset);
			offset += len + 4;

			return offset;
		}
	}
}
