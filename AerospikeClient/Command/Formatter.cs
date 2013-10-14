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
using System.Runtime.Serialization;

namespace Aerospike.Client
{
	/// <summary>
	/// This class contains the default formatter used when serializing objects to bytes.
	/// </summary>
	public sealed class Formatter
	{
		/// <summary>
		/// Default formatter used when serializing objects to bytes.
		/// The user can override this default.
		/// </summary>
		public static IFormatter Default = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
	}
}
