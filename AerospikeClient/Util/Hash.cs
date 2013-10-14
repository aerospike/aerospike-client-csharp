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
using System.Threading;
using System.Security.Cryptography;

namespace Aerospike.Client
{
	/// <summary>
	/// This class contains the algorithm used to create hash digests from user keys.
	/// </summary>
	public class Hash
	{
		private static readonly ThreadLocal<HashAlgorithm> ThreadLocalAlgorithm = new ThreadLocal<HashAlgorithm>(() =>
		{
			return RIPEMD160Managed.Create();
		});

		/// <summary>
		/// Get an instance of the hash algorithm, one instance per thread.
		/// </summary>
		public static HashAlgorithm Algorithm
		{
			get { return ThreadLocalAlgorithm.Value; }
		}
	}
}
