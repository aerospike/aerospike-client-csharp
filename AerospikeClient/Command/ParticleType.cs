/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
namespace Aerospike.Client
{
	public sealed class ParticleType
	{
		// Server particle types.
		public const int NULL = 0;
		public const int INTEGER = 1;
		public const int STRING = 3;
		public const int BLOB = 4;
		public const int CSHARP_BLOB = 8;
		public const int MAP = 19;
		public const int LIST = 20;
	}
}