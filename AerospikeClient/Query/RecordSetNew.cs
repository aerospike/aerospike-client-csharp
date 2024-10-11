/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	/// <summary>
	/// This class manages record retrieval from queries.
	/// Multiple threads will retrieve records from the server nodes and put these records on the queue.
	/// The single user thread consumes these records from the queue.
	/// </summary>
	public sealed class RecordSetNew : IAsyncDisposable, IAsyncEnumerable<KeyRecord>
	{
		public static readonly KeyRecord END = new(null, null);

		private readonly IAsyncEnumerable<KeyRecord> queue;
		private readonly CancellationToken cancelToken;

		/// <summary>
		/// Initialize record set with underlying producer/consumer queue.
		/// </summary>
		public RecordSetNew(int capacity, CancellationToken cancelToken)
		{
			//this.queue = new BlockingCollection<KeyRecord>(capacity);
			this.cancelToken = cancelToken;
		}

		//-------------------------------------------------------
		// Record traversal methods
		//-------------------------------------------------------

		
		public IAsyncEnumerator<KeyRecord> GetAsyncEnumerator(CancellationToken token)
		{
			return null;
		}
		
		public bool Disposed { get; private set; }
		private async Task DisposeAsync(bool disposing)
		{
			if (!Disposed)
			{
				if (disposing)
				{
					// TODO
				}

				Disposed = true;
			}
		}

		/// <summary>
		/// Close query
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			await DisposeAsync(disposing: true);
			GC.SuppressFinalize(this);
		}

		//-------------------------------------------------------
		// Meta-data retrieval methods
		//-------------------------------------------------------

		/// <summary>
		/// Get CancellationToken associated with this query.
		/// </summary>
		public CancellationToken CancelToken
		{
			get
			{
				return cancelToken;
			}
		}
	}
}
