/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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