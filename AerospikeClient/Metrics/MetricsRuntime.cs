/*
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Collections.ObjectModel;
using System.Diagnostics;
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	internal interface IMetricsClock
	{
		long GetTimestamp();

		long GetElapsedNanoseconds(long startTimestamp);
	}

	internal sealed class SystemMetricsClock : IMetricsClock
	{
		internal static readonly SystemMetricsClock Instance = new();

		private static readonly double NanosecondsPerTick = 1_000_000_000d / Stopwatch.Frequency;

		private SystemMetricsClock()
		{
		}

		public long GetTimestamp()
		{
			return Stopwatch.GetTimestamp();
		}

		public long GetElapsedNanoseconds(long startTimestamp)
		{
			double elapsed = (Stopwatch.GetTimestamp() - startTimestamp) * NanosecondsPerTick;
			return elapsed >= long.MaxValue ? long.MaxValue : (long)elapsed;
		}
	}

	internal sealed class MetricsProcessSampler
	{
		private readonly object sync = new();
		private readonly bool legacyMemory;
		private DateTime timestamp;
		private TimeSpan cpu;

		internal MetricsProcessSampler(bool legacyMemory = false)
		{
			this.legacyMemory = legacyMemory;
			using Process process = Process.GetCurrentProcess();
			timestamp = DateTime.UtcNow;
			cpu = process.TotalProcessorTime;
		}

		internal void Read(out double cpuPercent, out long memoryBytes)
		{
			lock (sync)
			{
				using Process process = Process.GetCurrentProcess();
				DateTime currentTimestamp = DateTime.UtcNow;
				TimeSpan currentCpu = process.TotalProcessorTime;
				double elapsedMs = (currentTimestamp - timestamp).TotalMilliseconds;

				cpuPercent = elapsedMs <= 0
					? 0
					: (currentCpu - cpu).TotalMilliseconds /
						(Environment.ProcessorCount * elapsedMs) * 100;
				memoryBytes = legacyMemory
					? process.WorkingSet64 +
						process.VirtualMemorySize64 +
						process.PagedMemorySize64
					: process.WorkingSet64;
				timestamp = currentTimestamp;
				cpu = currentCpu;
			}
		}

		internal void Reset()
		{
			lock (sync)
			{
				using Process process = Process.GetCurrentProcess();
				timestamp = DateTime.UtcNow;
				cpu = process.TotalProcessorTime;
			}
		}
	}

	internal interface IMetricsRandom
	{
		ulong NextUInt64();
	}

	internal sealed class ThreadMetricsRandom : IMetricsRandom
	{
		internal static readonly ThreadMetricsRandom Instance = new();

		private ThreadMetricsRandom()
		{
		}

		public ulong NextUInt64()
		{
			Span<byte> bytes = stackalloc byte[sizeof(ulong)];
			Random.Shared.NextBytes(bytes);
			return BitConverter.ToUInt64(bytes);
		}
	}

	internal sealed class MetricsRuntimeConfig
	{
		internal readonly long PolicyGeneration;
		internal readonly bool OperationalEnabled;
		internal readonly bool UsageEnabled;
		internal readonly ulong SampleRange;
		internal readonly ulong SampleThreshold;
		internal readonly MetricsHistogramShape Shape;

		internal MetricsRuntimeConfig(
			MetricsPolicy policy,
			long policyGeneration = 1,
			MetricsHistogramShape previousShape = null)
		{
			policy.Validate();

			PolicyGeneration = policyGeneration;
			OperationalEnabled = policy.OperationalEnabled;
			UsageEnabled = policy.UsageEnabled;
			SampleRange = policy.Sampler.Range;
			SampleThreshold = policy.Sampler.Threshold;
			Shape = previousShape?.Matches(policy) == true
				? previousShape
				: new MetricsHistogramShape(
					(previousShape?.Generation ?? 0) + 1,
					policy.LatencyUnit,
					policy.LatencyColumns,
					policy.LatencyShift);
		}
	}
}
