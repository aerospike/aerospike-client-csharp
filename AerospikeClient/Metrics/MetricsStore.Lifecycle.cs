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

namespace Aerospike.Client
{
	internal readonly struct MetricsWritePermit : IDisposable
	{
		private readonly MetricsStore store;

		internal MetricsWritePermit(MetricsStore store)
		{
			this.store = store;
		}

		internal bool Acquired => store != null;

		public void Dispose()
		{
			store?.ReleaseWrite();
		}
	}

	internal readonly struct MetricsBeginPermit : IDisposable
	{
		private readonly MetricsStore store;

		internal MetricsBeginPermit(MetricsStore store)
		{
			this.store = store;
		}

		internal bool Acquired => store != null;

		public void Dispose()
		{
			store?.ReleaseBegin();
		}
	}

	internal sealed partial class MetricsStore
	{
		internal MetricsBeginPermit AcquireBegin()
		{
			if (!CanBegin)
			{
				return default;
			}

			Interlocked.Increment(ref activeBegins);

			if (CanBegin)
			{
				return new MetricsBeginPermit(this);
			}

			Interlocked.Decrement(ref activeBegins);
			return default;
		}

		internal void ReleaseBegin()
		{
			Interlocked.Decrement(ref activeBegins);
		}

		internal MetricsWritePermit AcquireWrite()
		{
			if (Volatile.Read(ref lifecycleState) == (int)Lifecycle.Frozen)
			{
				return default;
			}

			Interlocked.Increment(ref activeWriters);

			if (Volatile.Read(ref lifecycleState) != (int)Lifecycle.Frozen)
			{
				return new MetricsWritePermit(this);
			}

			Interlocked.Decrement(ref activeWriters);
			return default;
		}

		internal MetricsWritePermit AcquireWrite(long sessionId)
		{
			return IsCurrentSession(sessionId) ? AcquireWrite() : default;
		}

		internal bool IsCurrentSession(long sessionId)
		{
			return sessionId != 0 &&
				Volatile.Read(ref SessionId) == sessionId;
		}

		internal void ReleaseWrite()
		{
			Interlocked.Decrement(ref activeWriters);
		}

		internal MetricsRuntimeConfig ApplyPolicy(MetricsPolicy policy)
		{
			MetricsRuntimeConfig current = config;
			MetricsRuntimeConfig replacement =
				new(policy, current.PolicyGeneration + 1, current.Shape);
			config = replacement;
			return replacement;
		}

		internal void Freeze()
		{
			Interlocked.Exchange(ref lifecycleState, (int)Lifecycle.Frozen);
			SpinWait spinner = new();

			while (Volatile.Read(ref activeBegins) != 0 ||
				Volatile.Read(ref activeWriters) != 0)
			{
				spinner.SpinOnce();
			}
		}

		internal bool Pause()
		{
			bool paused = Interlocked.CompareExchange(
				ref lifecycleState,
				(int)Lifecycle.Paused,
				(int)Lifecycle.Active) == (int)Lifecycle.Active;

			if (paused)
			{
				SpinWait spinner = new();

				while (Volatile.Read(ref activeBegins) != 0)
				{
					spinner.SpinOnce();
				}
			}
			return paused;
		}

		internal void Resume()
		{
			Interlocked.CompareExchange(
				ref lifecycleState,
				(int)Lifecycle.Active,
				(int)Lifecycle.Paused);
		}

		internal void StartSession()
		{
			MetricsRuntimeConfig current = config;
			SessionId = Interlocked.Increment(ref nextSessionId);
			StartedAt = DateTimeOffset.UtcNow;
			removedNodeCollector = new MetricsNodeCollector(current);
			callCollector = new MetricsNamespaceCollector(current.Shape);
			tendDuration = new HistogramState(current.Shape);
			lock (departureLock)
			{
				departures.Clear();
				departureTruncationGeneration = 0;
			}

			foreach (MetricsNodeGeneration generation in nodes.Values)
			{
				generation.ResetOperational(current);
			}

			Interlocked.Exchange(ref commandCount, 0);
			Interlocked.Exchange(ref commandRetries, 0);
			Interlocked.Exchange(ref delayQueueTimeouts, 0);
			Interlocked.Exchange(ref clientTimeouts, 0);
			Interlocked.Exchange(ref serverTimeouts, 0);
			Interlocked.Exchange(ref batchPartialFailures, 0);
			Interlocked.Exchange(ref invalidNodes, 0);
			Interlocked.Exchange(ref connectionOpenFailures, 0);
			Interlocked.Exchange(ref connectionTlsFailures, 0);
			Interlocked.Exchange(ref connectionAuthFailures, 0);
			Interlocked.Exchange(ref connectionsOpened, 0);
			Interlocked.Exchange(ref connectionsClosed, 0);
			Interlocked.Exchange(ref recoverEnqueued, 0);
			Interlocked.Exchange(ref recoverCompleted, 0);
			Interlocked.Exchange(ref recoverAborted, 0);
			Interlocked.Exchange(ref timeoutImmediate, 0);
			Interlocked.Exchange(ref queryRecordsQueued, 0);
			Interlocked.Exchange(ref queryServersConcurrent, 0);
			Interlocked.Exchange(ref queryAbandoned, 0);
			Interlocked.Exchange(ref apiBlocking, 0);
			Interlocked.Exchange(ref apiDeferred, 0);
			Interlocked.Exchange(ref apiBackground, 0);
			Volatile.Write(ref lifecycleState, (int)Lifecycle.Active);
		}
	}
}
