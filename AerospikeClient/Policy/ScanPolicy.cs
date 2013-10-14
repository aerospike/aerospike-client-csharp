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
	/// <summary>
	/// Container object for optional parameters used in scan operations.
	/// </summary>
	public sealed class ScanPolicy : Policy
	{
		/// <summary>
		/// Fraction of data to scan - not yet supported.
		/// </summary>
		public int scanPercent = 100;

		/// <summary>
		/// Issue scan requests in parallel or serially. 
		/// </summary>
		public bool concurrentNodes = true;

		/// <summary>
		/// Indicates if bin data is retrieved. If false, only record digests are retrieved.
		/// </summary>
		public bool includeBinData = true;

		/// <summary>
		/// Terminate scan if cluster in fluctuating state.
		/// </summary>
		public bool failOnClusterChange;
	}
}