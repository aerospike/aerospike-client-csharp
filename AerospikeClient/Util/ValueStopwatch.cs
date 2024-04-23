/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Diagnostics;

namespace Aerospike.Client
{
	/// <summary>
	/// <see cref="Stopwatch"/> but as a struct.
	/// </summary>
	public readonly struct ValueStopwatch
	{
		private static readonly double TimespanTicksPerStopwatchTick = TimeSpan.TicksPerSecond / (double)Stopwatch.Frequency;
		private static readonly double MillisecondsPerStopwatchTick = TimespanTicksPerStopwatchTick / TimeSpan.TicksPerMillisecond;

		public static ValueStopwatch StartNew() => new(Stopwatch.GetTimestamp());

		private readonly long startTimestamp;

		public bool IsActive => startTimestamp != 0;

		public TimeSpan Elapsed => new((long)((Stopwatch.GetTimestamp() - startTimestamp) * TimespanTicksPerStopwatchTick));

		public long ElapsedMilliseconds => (long)((Stopwatch.GetTimestamp() - startTimestamp) * MillisecondsPerStopwatchTick);

		private ValueStopwatch(long startTimestamp) => this.startTimestamp = startTimestamp;
    }
}
