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
	public sealed class FieldType
	{
		public const int NAMESPACE = 0;
		public const int TABLE = 1;
		public const int KEY = 2;
		public const int DIGEST_RIPE = 4;
		public const int DIGEST_RIPE_ARRAY = 6;
		public const int TRAN_ID = 7; // user supplied transaction id, which is simply passed back
		public const int SCAN_OPTIONS = 8;
		public const int INDEX_NAME = 21;
		public const int INDEX_RANGE = 22;
		public const int INDEX_FILTER = 23;
		public const int INDEX_LIMIT = 24;
		public const int INDEX_ORDER_BY = 25;
		public const int UDF_PACKAGE_NAME = 30;
		public const int UDF_FUNCTION = 31;
		public const int UDF_ARGLIST = 32;
		public const int UDF_OP = 33;
		public const int QUERY_BINLIST = 40;
	}
}