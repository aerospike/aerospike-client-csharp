/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

namespace Aerospike.Client
{
	/// <summary>
	/// A Counter is a container for a namespace-aggregate map of long counters
	/// </summary>
	public class Counter : IDisposable
	{
		private readonly ConcurrentHashMap<string, long?> counterMap = new();
		private readonly static string noNsLabel = "";
		private bool disposedValue;

		public Counter() { }

		/// <summary>
		/// Increment the counter by 1 for the given namespace
		/// </summary>
		/// <param name="ns">the namespace for the counter</param>
		public void Increment(string ns)
		{
			string nsLabel = ns ?? noNsLabel;
			counterMap.FindElementAndPerformWriteFunc(nsLabel, (k, found, v) =>
			{
				if (!found) // key is not found, so add it
				{
					return 1;
				}
				else
				{
					return v + 1;
				}
			});
		}

		/// <summary>
		/// Increment the counter by the provided amount for the given namespace
		/// </summary>
		/// <param name="ns">the namespace for the counter</param>
		/// <param name="count"></param>
		public void Increment(string ns, long count)
		{
			string nsLabel = ns ?? noNsLabel;
			counterMap.FindElementAndPerformWriteFunc(nsLabel, (k, found, v) =>
			{
				if (!found) // key is not found, so add it
				{
					return count;
				}
				else
				{
					return v + count;
				}
			});
		}

		/// <summary>
		/// Get the counter's total, which is the sum of the counter across all namespaces
		/// </summary>
		/// <returns>the total</returns>
		public long GetTotal()
		{
			long total = 0;
			foreach (string ns in counterMap.Keys)
			{
				try
				{
					total += counterMap[ns] ?? 0;
				}
				catch (KeyNotFoundException)
				{
					total += 0;
				}
			}
			return total;
		}

		/// <summary>
		/// Get the counter's count for the provided namespace
		/// </summary>
		/// <param name="ns">the namespace for which to get the count</param>
		/// <returns>the count for the namesapce</returns>
		public long GetCountByNS(string ns)
		{
			if (counterMap.ContainsKey(ns))
			{
				return counterMap[ns].Value;
			}
			else
			{
				return 0;
			}
		}

		protected virtual void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					counterMap.Dispose();
				}

				disposedValue = true;
			}
		}

		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}
	}
}
