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

using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	internal sealed partial class MetricsStore
	{
		internal long StartTend()
		{
			using MetricsBeginPermit permit = AcquireBegin();

			if (!permit.Acquired)
			{
				return long.MinValue;
			}
			return config.OperationalEnabled ? Clock.GetTimestamp() : 0;
		}

		internal long StartConnection()
		{
			return TryStartConnection(out long startTimestamp, out _)
				? startTimestamp
				: 0;
		}

		internal bool TryStartConnection(out long startTimestamp)
		{
			return TryStartConnection(out startTimestamp, out _);
		}

		internal bool TryStartConnection(
			out long startTimestamp,
			out MetricsRuntimeConfig runtimeConfig)
		{
			using MetricsBeginPermit permit = AcquireBegin();

			if (!permit.Acquired)
			{
				startTimestamp = 0;
				runtimeConfig = null;
				return false;
			}

			runtimeConfig = config;
			startTimestamp = runtimeConfig.OperationalEnabled
				? Clock.GetTimestamp()
				: 0;
			return true;
		}

		internal bool TryStartRecover()
		{
			using MetricsBeginPermit permit = AcquireBegin();
			return permit.Acquired && config.OperationalEnabled;
		}

		internal void CompleteConnection(Node node, string ns, long startTimestamp)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				CompleteConnectionAdmitted(node, ns, startTimestamp, config);
			}
		}

		internal void CompleteConnectionAdmitted(
			Node node,
			string ns,
			long startTimestamp,
			MetricsRuntimeConfig runtimeConfig)
		{
			if (runtimeConfig.OperationalEnabled && startTimestamp != 0)
			{
				long elapsedNanoseconds =
					Clock.GetElapsedNanoseconds(startTimestamp);
				using NodeMetricsLease lease = node.AcquireMetrics();
				if (lease.Metrics != null)
				{
					lease.Metrics.AddLatency(
						string.Empty,
						LatencyType.CONN,
						elapsedNanoseconds / 1_000_000.0);
				}
				else
				{
					GetNode(node, runtimeConfig).AddConnectionLatency(
						elapsedNanoseconds,
						runtimeConfig.Shape);
				}
			}
		}

		internal void CompleteTend(long startTimestamp, int invalidCount)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (!permit.Acquired)
			{
				return;
			}

			if (invalidCount > 0)
			{
				Interlocked.Add(ref invalidNodes, invalidCount);
			}

			if (startTimestamp != 0)
			{
				EnsureTend(config).Histogram.AddNanoseconds(
					Clock.GetElapsedNanoseconds(startTimestamp));
			}
		}

		// Write-path forward-only guard; snapshots copy tendDuration directly.
		private HistogramState EnsureTend(MetricsRuntimeConfig expected)
		{
			HistogramState current = tendDuration;

			if (!current.Shape.ShouldReplaceWith(expected.Shape))
			{
				return current;
			}

			lock (tendLock)
			{
				current = tendDuration;

				if (current.Shape.ShouldReplaceWith(expected.Shape))
				{
					current = new HistogramState(expected.Shape);
					tendDuration = current;
				}
				return current;
			}
		}

		internal void AddConnectionFailure(ConnectionFailurePhase phase, bool opening)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (!permit.Acquired)
			{
				return;
			}
			AddConnectionFailureAdmitted(phase, opening);
		}

		internal void AddConnectionFailureAdmitted(
			ConnectionFailurePhase phase,
			bool opening)
		{
			if (!config.OperationalEnabled)
			{
				return;
			}
			AddCapturedConnectionFailureAdmitted(phase, opening);
		}

		internal void AddCapturedConnectionFailureAdmitted(
			ConnectionFailurePhase phase,
			bool opening)
		{
			if (opening)
			{
				Interlocked.Increment(ref connectionOpenFailures);
			}

			if (phase == ConnectionFailurePhase.Tls)
			{
				Interlocked.Increment(ref connectionTlsFailures);
			}
			else if (phase == ConnectionFailurePhase.Auth)
			{
				Interlocked.Increment(ref connectionAuthFailures);
			}
		}

		internal void AddNodeConnectionFailureAdmitted(
			Node node,
			ConnectionFailurePhase phase,
			bool opening)
		{
			if (config.OperationalEnabled && node != null)
			{
				GetNode(node).AddConnectionFailure(phase, opening);
			}
		}

		internal void AddRecoverEnqueued()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				AddRecoverEnqueuedAdmitted();
			}
		}

		internal void AddRecoverEnqueuedAdmitted()
		{
			if (config.OperationalEnabled)
			{
				AddCapturedRecoverEnqueuedAdmitted();
			}
		}

		internal void AddCapturedRecoverEnqueuedAdmitted()
		{
			Interlocked.Increment(ref recoverEnqueued);
		}

		internal void AddRecoverCompleted()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				AddRecoverCompletedAdmitted();
			}
		}

		internal void AddRecoverCompletedAdmitted()
		{
			if (config.OperationalEnabled)
			{
				AddCapturedRecoverCompletedAdmitted();
			}
		}

		internal void AddCapturedRecoverCompletedAdmitted()
		{
			Interlocked.Increment(ref recoverCompleted);
		}

		internal void AddRecoverAborted()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				AddRecoverAbortedAdmitted();
			}
		}

		internal void AddRecoverAbortedAdmitted()
		{
			if (config.OperationalEnabled)
			{
				AddCapturedRecoverAbortedAdmitted();
			}
		}

		internal void AddCapturedRecoverAbortedAdmitted()
		{
			Interlocked.Increment(ref recoverAborted);
		}

		internal void AddImmediateTimeout()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired && config.OperationalEnabled)
			{
				Interlocked.Increment(ref timeoutImmediate);
			}
		}

		internal void AddQueryRecordsQueued(int delta)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				Interlocked.Add(ref queryRecordsQueued, delta);
			}
		}

		internal void AddQueryServersConcurrent(int delta)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				Interlocked.Add(ref queryServersConcurrent, delta);
			}
		}

		internal void AddQueryAbandoned()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				Interlocked.Increment(ref queryAbandoned);
			}
		}

		internal void AddConnectionOpened(Node node)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				AddConnectionOpenedAdmitted(node);
			}
		}

		internal void AddConnectionOpenedAdmitted(Node node)
		{
			Interlocked.Increment(ref connectionsOpened);
			GetNode(node).AddOpened();
		}

		internal void AddConnectionClosed(Node node)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				Interlocked.Increment(ref connectionsClosed);
				GetNode(node).AddClosed();
			}
		}

		internal void AddPoolExhausted(Node node)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired && config.OperationalEnabled)
			{
				GetNode(node).AddPoolExhausted();
			}
		}

		internal void AddUsage(ApiExecutionMode mode)
		{
			using MetricsBeginPermit permit = AcquireBegin();

			if (!permit.Acquired)
			{
				return;
			}
			AddUsageAdmitted(mode, config);
		}

		private void AddUsageAdmitted(
			ApiExecutionMode mode,
			MetricsRuntimeConfig current)
		{
			if (!current.UsageEnabled)
			{
				return;
			}

			switch (mode)
			{
				case ApiExecutionMode.Blocking:
					Interlocked.Increment(ref apiBlocking);
					break;

				case ApiExecutionMode.Deferred:
					Interlocked.Increment(ref apiDeferred);
					break;

				case ApiExecutionMode.Background:
					Interlocked.Increment(ref apiBackground);
					break;
			}
		}
	}
}
