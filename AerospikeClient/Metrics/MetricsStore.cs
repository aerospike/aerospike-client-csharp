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
using System.Diagnostics;

namespace Aerospike.Client
{
	internal sealed partial class MetricsStore
	{
		private const int MaximumDepartureNames = 256;

		private enum Lifecycle
		{
			Frozen,
			Active,
			Paused
		}

		private sealed class HistogramState
		{
			internal readonly MetricsHistogramShape Shape;
			internal readonly MetricsHistogram Histogram;

			internal HistogramState(MetricsHistogramShape shape)
			{
				Shape = shape;
				Histogram = new MetricsHistogram(shape);
			}
		}

		private sealed class DepartureEntry
		{
			internal readonly string Name;
			internal readonly MetricsNodeNameState State;
			internal string Address;
			internal int Port;
			internal int ErrorRate;
			internal long Generation;
			internal MetricsNodeView Acknowledged;
			internal MetricsNodeCompatibilitySnapshot Compatibility;

			internal DepartureEntry(
				string name,
				MetricsNodeNameState state)
			{
				Name = name;
				State = state;
			}
		}

		private static long nextSessionId;
		private readonly ConcurrentDictionary<Node, MetricsNodeGeneration> nodes =
			new(ReferenceEqualityComparer.Instance);
		private readonly ConcurrentDictionary<string, MetricsNodeNameState> nodeNames =
			new(StringComparer.Ordinal);
		private readonly object departureLock = new();
		private readonly Dictionary<string, DepartureEntry> departures =
			new(StringComparer.Ordinal);
		private long departureGeneration;
		private long departureTruncationGeneration;
		private MetricsNodeCollector removedNodeCollector;
		private MetricsNamespaceCollector callCollector;
		private volatile MetricsRuntimeConfig config;
		private readonly IMetricsRandom random;
		private readonly object processLock = new();
		private readonly object tendLock = new();
		private DateTime processTimestamp;
		private TimeSpan processCpu;
		private int lifecycleState = (int)Lifecycle.Active;
		private int activeBegins;
		private int activeWriters;
		private long commandCount;
		private long commandRetries;
		private long delayQueueTimeouts;
		private long clientTimeouts;
		private long serverTimeouts;
		private long batchPartialFailures;
		private long invalidNodes;
		private long connectionOpenFailures;
		private long connectionTlsFailures;
		private long connectionAuthFailures;
		private long connectionsOpened;
		private long connectionsClosed;
		private long recoverEnqueued;
		private long recoverCompleted;
		private long recoverAborted;
		private long timeoutImmediate;
		private long queryRecordsQueued;
		private long queryServersConcurrent;
		private long queryAbandoned;
		private long apiBlocking;
		private long apiDeferred;
		private long apiBackground;

		internal readonly IMetricsClock Clock;
		internal long SessionId;
		internal DateTimeOffset StartedAt;
		private volatile HistogramState tendDuration;

		internal bool Active =>
			Volatile.Read(ref lifecycleState) == (int)Lifecycle.Active;
		internal bool CanBegin =>
			Volatile.Read(ref lifecycleState) == (int)Lifecycle.Active;
		internal bool AcceptsWrites =>
			Volatile.Read(ref lifecycleState) != (int)Lifecycle.Frozen;
		internal bool OperationalEnabled => CanBegin && config.OperationalEnabled;

		internal MetricsRuntimeConfig RuntimeConfig => config;


		internal MetricsStore(
			MetricsPolicy policy,
			IMetricsClock clock = null,
			IMetricsRandom random = null,
			bool active = true)
		{
			SessionId = Interlocked.Increment(ref nextSessionId);
			StartedAt = DateTimeOffset.UtcNow;
			config = new MetricsRuntimeConfig(policy);
			removedNodeCollector = new MetricsNodeCollector(config);
			callCollector = new MetricsNamespaceCollector(config.Shape);
			Clock = clock ?? SystemMetricsClock.Instance;
			this.random = random ?? ThreadMetricsRandom.Instance;
			tendDuration = new HistogramState(config.Shape);
			lifecycleState = active
				? (int)Lifecycle.Active
				: (int)Lifecycle.Frozen;
			using Process process = Process.GetCurrentProcess();
			processTimestamp = DateTime.UtcNow;
			processCpu = process.TotalProcessorTime;
		}

		private bool Sample(MetricsRuntimeConfig current)
		{
			if (current.SampleThreshold == 0)
			{
				return false;
			}
			return current.SampleThreshold == current.SampleRange ||
				(random.NextUInt64() % current.SampleRange) < current.SampleThreshold;
		}
	}
}
