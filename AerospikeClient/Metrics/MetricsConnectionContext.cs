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
	internal enum ConnectionMetricsOrigin
	{
		Validation,
		NodePool
	}

	internal sealed class ConnectionMetricsState
	{
		private readonly MetricsStore store;
		private readonly MetricsRuntimeConfig runtimeConfig;
		private readonly long startTimestamp;
		private readonly long sessionId;
		private readonly ConnectionMetricsOrigin origin;
		private readonly Node node;
		private int state;

		private const int TerminalMask = 3;
		private const int Succeeded = 1;
		private const int Failed = 2;
		private const int Canceled = 3;
		private const int TlsRecorded = 4;
		private const int AuthRecorded = 8;

		internal ConnectionMetricsState(
			MetricsStore store,
			MetricsRuntimeConfig runtimeConfig,
			long startTimestamp,
			ConnectionMetricsOrigin origin,
			Node node)
		{
			this.store = store;
			this.runtimeConfig = runtimeConfig;
			this.startTimestamp = startTimestamp;
			sessionId = store.SessionId;
			this.origin = origin;
			this.node = node;
		}

		internal void Complete(Node completedNode, string ns)
		{
			using MetricsWritePermit permit = store.AcquireWrite(sessionId);

			if (permit.Acquired &&
				TrySetTerminal(Succeeded))
			{
				Node target = node ?? completedNode;
				store.AddConnectionOpenedAdmitted(target);
				store.CompleteConnectionAdmitted(
					target,
					string.Empty,
					startTimestamp,
					runtimeConfig);
			}
		}

		internal void Fail(ConnectionFailurePhase phase)
		{
			using MetricsWritePermit permit = store.AcquireWrite(sessionId);

			if (permit.Acquired && TrySetTerminal(Failed))
			{
				if (runtimeConfig.OperationalEnabled)
				{
					RecordOpenFailure();
					RecordPhase(phase);
				}
			}
		}

		internal void FailPhase(ConnectionFailurePhase phase)
		{
			using MetricsWritePermit permit = store.AcquireWrite(sessionId);

			if (permit.Acquired &&
				(Volatile.Read(ref state) & TerminalMask) == 0)
			{
				RecordPhase(phase);
			}
		}

		internal void Cancel()
		{
			TrySetTerminal(Canceled);
		}

		private bool TrySetTerminal(int terminal)
		{
			while (true)
			{
				int current = Volatile.Read(ref state);

				if ((current & TerminalMask) != 0)
				{
					return false;
				}

				if (Interlocked.CompareExchange(
					ref state,
					current | terminal,
					current) == current)
				{
					return true;
				}
			}
		}

		private void RecordPhase(ConnectionFailurePhase phase)
		{
			if (!runtimeConfig.OperationalEnabled)
			{
				return;
			}

			int bit = phase switch
			{
				ConnectionFailurePhase.Tls => TlsRecorded,
				ConnectionFailurePhase.Auth => AuthRecorded,
				_ => 0
			};

			if (bit == 0)
			{
				return;
			}

			while (true)
			{
				int current = Volatile.Read(ref state);

				if ((current & bit) != 0)
				{
					return;
				}

				if (Interlocked.CompareExchange(
					ref state,
					current | bit,
					current) == current)
				{
					RecordFailure(phase, false);
					return;
				}
			}
		}

		private void RecordOpenFailure()
		{
			RecordFailure(ConnectionFailurePhase.Open, true);
		}

		private void RecordFailure(ConnectionFailurePhase phase, bool opening)
		{
			if (origin == ConnectionMetricsOrigin.NodePool)
			{
				store.AddNodeConnectionFailureAdmitted(node, phase, opening);
			}
			else
			{
				store.AddCapturedConnectionFailureAdmitted(phase, opening);
			}
		}
	}

	internal readonly struct ConnectionMetricsHandle
	{
		private readonly ConnectionMetricsState state;

		internal bool Enabled => state != null;

		internal ConnectionMetricsHandle(
			MetricsStore store,
			long startTimestamp)
			: this(
				store,
				store?.RuntimeConfig,
				startTimestamp,
				ConnectionMetricsOrigin.Validation,
				null)
		{
		}

		internal ConnectionMetricsHandle(
			MetricsStore store,
			MetricsRuntimeConfig runtimeConfig,
			long startTimestamp,
			ConnectionMetricsOrigin origin = ConnectionMetricsOrigin.Validation,
			Node node = null)
		{
			state = store == null
				? null
				: new ConnectionMetricsState(
					store,
					runtimeConfig,
					startTimestamp,
					origin,
					node);
		}

		internal void Complete(Node node, string ns)
		{
			state?.Complete(node, ns);
		}

		internal void Fail(ConnectionFailurePhase phase)
		{
			state?.Fail(phase);
		}

		internal void FailPhase(ConnectionFailurePhase phase)
		{
			state?.FailPhase(phase);
		}

		internal void Cancel()
		{
			state?.Cancel();
		}
	}

	internal sealed class RecoverMetricsState
	{
		private readonly MetricsStore store;
		private int state;

		internal RecoverMetricsState(MetricsStore store)
		{
			this.store = store;
		}

		internal void Enqueue()
		{
			using MetricsWritePermit permit = store.AcquireWrite();

			if (permit.Acquired &&
				Interlocked.CompareExchange(ref state, 1, 0) == 0)
			{
				store.AddCapturedRecoverEnqueuedAdmitted();
			}
		}

		internal void Complete()
		{
			using MetricsWritePermit permit = store.AcquireWrite();

			if (permit.Acquired &&
				Interlocked.CompareExchange(ref state, 2, 1) == 1)
			{
				store.AddCapturedRecoverCompletedAdmitted();
			}
		}

		internal void Abort()
		{
			using MetricsWritePermit permit = store.AcquireWrite();

			if (permit.Acquired)
			{
				while (true)
				{
					int current = Volatile.Read(ref state);

					if (current == 2 || current == 3)
					{
						return;
					}

					if (Interlocked.CompareExchange(ref state, 3, current) == current)
					{
						store.AddCapturedRecoverAbortedAdmitted();
						return;
					}
				}
			}
		}
	}

	internal readonly struct RecoverMetricsHandle
	{
		private readonly RecoverMetricsState state;

		internal RecoverMetricsHandle(MetricsStore store)
		{
			state = store == null ? null : new RecoverMetricsState(store);
		}

		internal bool Enabled => state != null;

		internal void Enqueue()
		{
			state?.Enqueue();
		}

		internal void Complete()
		{
			state?.Complete();
		}

		internal void Abort()
		{
			state?.Abort();
		}
	}
}
