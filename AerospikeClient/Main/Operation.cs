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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Database operation definition.  The class is used in client's operate() method. 
	/// </summary>
	public sealed class Operation
	{
		/// <summary>
		/// Create read bin database operation.
		/// </summary>
		public static Operation Get(string binName)
		{
			return new Operation(Type.READ, binName);
		}

		/// <summary>
		/// Create read all record bins database operation.
		/// </summary>
		public static Operation Get()
		{
			return new Operation(Type.READ);
		}

		/// <summary>
		/// Create read record header database operation.
		/// </summary>
		public static Operation GetHeader()
		{
			return new Operation(Type.READ_HEADER);
		}

		/// <summary>
		/// Create set database operation.
		/// </summary>
		public static Operation Put(Bin bin)
		{
			return new Operation(Type.WRITE, bin);
		}

		/// <summary>
		/// Create string append database operation.
		/// </summary>
		public static Operation Append(Bin bin)
		{
			return new Operation(Type.APPEND, bin);
		}

		/// <summary>
		/// Create string prepend database operation.
		/// </summary>
		public static Operation Prepend(Bin bin)
		{
			return new Operation(Type.PREPEND, bin);
		}

		/// <summary>
		/// Create integer add database operation.
		/// </summary>
		public static Operation Add(Bin bin)
		{
			return new Operation(Type.ADD, bin);
		}

		/// <summary>
		/// Create touch database operation.
		/// </summary>
		public static Operation Touch()
		{
			return new Operation(Type.TOUCH);
		}

		public enum Type
		{
			READ,
			READ_HEADER,
			WRITE,
			ADD,
			APPEND,
			PREPEND,
			TOUCH
		}

		private static byte[] ProtocolTypes = new byte[] { 1, 1, 2, 5, 9, 10, 11 };

		public static byte GetProtocolType(Type type)
		{
			return ProtocolTypes[(int)type];
		}

		/// <summary>
		/// Type of operation.
		/// </summary>
		public readonly Type type;

		/// <summary>
		/// Optional bin name used in operation.
		/// </summary>
		public readonly string binName;

		/// <summary>
		/// Optional bin value used in operation.
		/// </summary>
		public readonly Value binValue;

		private Operation(Type type, Bin bin)
		{
			this.type = type;
			this.binName = bin.name;
			this.binValue = bin.value;
		}

		private Operation(Type type, string binName)
		{
			this.type = type;
			this.binName = binName;
			this.binValue = Value.AsNull;
		}

		private Operation(Type type)
		{
			this.type = type;
			this.binName = null;
			this.binValue = Value.AsNull;
		}
	}
}
