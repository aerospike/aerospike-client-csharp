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
using System;
using System.Text;
using System.Threading;

namespace Aerospike.Demo
{
	public sealed class LatencyManager : ILatencyManager
	{
		private readonly Bucket[] buckets;
		private readonly int lastBucket;
		private readonly int bitShift;

		public LatencyManager(int columns, int bitShift)
		{
			this.lastBucket = columns - 1;
			this.bitShift = bitShift;
			buckets = new Bucket[columns];

			for (int i = 0; i < buckets.Length; i++)
			{
				buckets[i] = new Bucket();
			}
		}

		public void Add(double elapsed)
		{
			int index = GetIndex(elapsed);
			buckets[index].Increment();
		}

		private int GetIndex(double elapsed)
		{
			int e = (int)Math.Ceiling(elapsed);
			int limit = 1;

			for (int i = 0; i < lastBucket; i++)
			{
				if (e <= limit)
				{
					return i;
				}
				limit <<= bitShift;
			}
			return lastBucket;
		}

		public string PrintHeader()
		{
			StringBuilder sb = new StringBuilder(200);
			int limit = 1;
			sb.Append("      <=1ms >1ms");

			for (int i = 2; i <= lastBucket; i++)
			{
				limit <<= bitShift;
				String s = " >" + limit + "ms";
				sb.Append(s);
			}
			return sb.ToString();
		}

		/// <summary>
		/// Print latency percents for specified cumulative ranges.
		/// This function is not absolutely accurate for a given time slice because this method 
		/// is not synchronized with the Add() method.  Some values will slip into the next iteration.  
		/// It is not a good idea to add extra locks just to measure performance since that actually 
		/// affects performance.  Fortunately, the values will even out over time
		/// (ie. no double counting).
		/// </summary>
		public string PrintResults(StringBuilder sb, string prefix)
		{
			// Capture snapshot and make buckets cumulative.
			int[] array = new int[buckets.Length];
			int sum = 0;
			int count;

			for (int i = buckets.Length - 1; i >= 1; i--)
			{
				count = buckets[i].Reset();
				array[i] = count + sum;
				sum += count;
			}
			// The first bucket (<=1ms) does not need a cumulative adjustment.
			count = buckets[0].Reset();
			array[0] = count;
			sum += count;

			// Print cumulative results.
			sb.Length = 0;
			sb.Append(prefix);
			int spaces = 6 - prefix.Length;

			for (int j = 0; j < spaces; j++)
			{
				sb.Append(' ');
			}

			double sumDouble = (double)sum;
			int limit = 1;

			PrintColumn(sb, limit, sumDouble, array[0]);
			PrintColumn(sb, limit, sumDouble, array[1]);

			for (int i = 2; i < array.Length; i++)
			{
				limit <<= bitShift;
				PrintColumn(sb, limit, sumDouble, array[i]);
			}
			return sb.ToString();
		}

		public string PrintSummary(StringBuilder sb, string prefix)
		{
			int[] array = new int[buckets.Length];
			int sum = 0;
			int count;

			for (int i = buckets.Length - 1; i >= 1; i--)
			{
				count = buckets[i].sum;
				array[i] = count + sum;
				sum += count;
			}
			// The first bucket (<=1ms) does not need a cumulative adjustment.
			count = buckets[0].sum;
			array[0] = count;
			sum += count;

			// Print cumulative results.
			sb.Length = 0;
			sb.Append(prefix);
			int spaces = 6 - prefix.Length;

			for (int j = 0; j < spaces; j++)
			{
				sb.Append(' ');
			}

			double sumDouble = (double)sum;
			int limit = 1;

			PrintColumn(sb, limit, sumDouble, array[0]);
			PrintColumn(sb, limit, sumDouble, array[1]);

			for (int i = 2; i < array.Length; i++)
			{
				limit <<= bitShift;
				PrintColumn(sb, limit, sumDouble, array[i]);
			}
			return sb.ToString();
		}

		private void PrintColumn(StringBuilder sb, int limit, double sum, int value)
		{
			int percent = 0;

			if (value > 0)
			{
				percent = (int)((double)value * 100.0 / sum + 0.5);
			}
			string percentString = Convert.ToString(percent) + "%";
			int spaces = limit.ToString().Length + 4 - percentString.Length;

			for (int j = 0; j < spaces; j++)
			{
				sb.Append(' ');
			}
			sb.Append(percentString);
		}

		private sealed class Bucket
		{
			int count = 0;
			public int sum = 0;

			public void Increment()
			{
				Interlocked.Increment(ref count);
			}

			public int Reset()
			{
				int c = Interlocked.Exchange(ref count, 0);
				sum += c;
				return c;
			}
		}
	}
}
