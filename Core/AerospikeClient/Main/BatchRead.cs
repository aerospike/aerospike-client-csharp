/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Key and bin names used in batch commands where variables bins are needed for each key.
	/// </summary>
	public sealed class BatchRead
	{
		/// <summary>
		/// Key.
		/// </summary>
		public readonly Key key;

		/// <summary>
		/// Bins to retrieve for this key.
		/// </summary>
		public readonly string[] binNames;

		/// <summary>
		/// If true, ignore binNames and read all bins.
		/// If false and binNames are set, read specified binNames.
		/// If false and binNames are not set, read record header (generation, expiration) only.
		/// </summary>
		public readonly bool readAllBins;

		/// <summary>
		/// Record result after batch command has completed.  Will be null if record was not found.
		/// </summary>
		public Record record;

		/// <summary>
		/// Initialize batch key and bins to retrieve.
		/// </summary>
		/// <param name="key">record key</param>
		/// <param name="binNames">array of bins to retrieve.</param>
		public BatchRead(Key key, string[] binNames)
		{
			this.key = key;
			this.binNames = binNames;
			this.readAllBins = false;
		}

		/// <summary>
		/// Initialize batch key and readAllBins indicator.
		/// </summary>
		/// <param name="key">record key</param>
		/// <param name="readAllBins">should all bins in record be retrieved.</param>
		public BatchRead(Key key, bool readAllBins)
		{
			this.key = key;
			this.binNames = null;
			this.readAllBins = readAllBins;
		}

		/// <summary>
		/// Convert BatchRecord to string.
		/// </summary>
		public override string ToString()
		{
			return key.ToString() + ":" + Util.ArrayToString(binNames);
		}
	}
}
