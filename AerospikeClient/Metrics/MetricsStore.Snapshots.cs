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

using System.Collections.ObjectModel;
using System.Diagnostics;

namespace Aerospike.Client
{
	internal sealed partial class MetricsStore
	{
		internal MetricsStoreSnapshot Read(MetricsRuntimeConfig expected = null)
		{
			while (true)
			{
				MetricsRuntimeConfig current = expected ?? config;

				if (!ReferenceEquals(config, current))
				{
					if (expected != null)
					{
						return null;
					}
					continue;
				}
				Dictionary<string, MetricsNodeView> nodeViews = new();
				Dictionary<string, MetricsNodeView> compatibilityViews = new();
				Dictionary<string, MetricsNodeCapture> nodeCaptures = new();
				MetricsNodeCollector removedAggregate = new(current);

				foreach (KeyValuePair<string, MetricsNodeNameState> entry in nodeNames)
				{
					MetricsNodeCapture capture =
						entry.Value.Capture(entry.Key, current);
					MetricsNodeView nodeView = capture.Public;
					nodeCaptures[entry.Key] = capture;

					if (capture.Active &&
						!MetricsNodeNameState.IsZero(nodeView))
					{
						nodeViews[entry.Key] = nodeView;
					}
					if (capture.Active)
					{
						compatibilityViews[entry.Key] =
							capture.Compatibility;
					}
					else
					{
						removedAggregate.Merge(nodeView, current);
					}
				}

				removedAggregate.Merge(
					removedNodeCollector.Read(string.Empty, current),
					current);

				MetricsNodeView removedNodeView =
					removedAggregate.Read(string.Empty, current);
				IReadOnlyList<long> tendBuckets =
					Array.AsReadOnly(EnsureTend(current).Histogram.CopyBuckets());
				MetricsDepartureCapture departureCapture =
					CaptureDepartures(current, nodeCaptures);

				if (!ReferenceEquals(config, current))
				{
					if (expected != null)
					{
						return null;
					}
					continue;
				}

				return new MetricsStoreSnapshot(
				SessionId,
				StartedAt,
				current.PolicyGeneration,
				current.Shape.Generation,
				Active,
				current.OperationalEnabled,
				current.UsageEnabled,
				current.Shape.Unit,
				current.Shape.Columns,
				current.Shape.Shift,
				current.SampleRange,
				current.SampleThreshold,
				Volatile.Read(ref commandCount),
				Volatile.Read(ref commandRetries),
				Volatile.Read(ref delayQueueTimeouts),
				Volatile.Read(ref clientTimeouts),
				Volatile.Read(ref serverTimeouts),
				Volatile.Read(ref batchPartialFailures),
				Volatile.Read(ref invalidNodes),
				Volatile.Read(ref connectionOpenFailures),
				Volatile.Read(ref connectionTlsFailures),
				Volatile.Read(ref connectionAuthFailures),
				Volatile.Read(ref connectionsOpened),
				Volatile.Read(ref connectionsClosed),
				Volatile.Read(ref recoverEnqueued),
				Volatile.Read(ref recoverCompleted),
				Volatile.Read(ref recoverAborted),
				Volatile.Read(ref timeoutImmediate),
				Volatile.Read(ref queryRecordsQueued),
				Volatile.Read(ref queryServersConcurrent),
				Volatile.Read(ref queryAbandoned),
				tendBuckets,
				Volatile.Read(ref apiBlocking),
				Volatile.Read(ref apiDeferred),
				Volatile.Read(ref apiBackground),
				new ReadOnlyDictionary<string, MetricsNodeView>(nodeViews),
				removedNodeView)
				{
					CallLatency = callCollector.Read(
						string.Empty,
						current).Latency,
					CompatibilityNodes =
						new ReadOnlyDictionary<string, MetricsNodeView>(
							compatibilityViews),
					Departures = departureCapture
				};
			}
		}

		internal void ReadProcess(out double cpuPercent, out long memoryBytes)
		{
			lock (processLock)
			{
				using Process process = Process.GetCurrentProcess();
				DateTime timestamp = DateTime.UtcNow;
				TimeSpan cpu = process.TotalProcessorTime;
				double elapsedMs = (timestamp - processTimestamp).TotalMilliseconds;

				cpuPercent = elapsedMs <= 0
					? 0
					: (cpu - processCpu).TotalMilliseconds /
						(Environment.ProcessorCount * elapsedMs) * 100;
				memoryBytes = process.WorkingSet64;
				processTimestamp = timestamp;
				processCpu = cpu;
			}
		}
	}
}
