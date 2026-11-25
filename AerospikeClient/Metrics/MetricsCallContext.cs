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
	internal enum MetricsFailure
	{
		None,
		Timeout,
		KeyBusy,
		RecordTooBig,
		DeviceOverload,
		Error
	}

	internal enum ApiExecutionMode
	{
		Blocking,
		Deferred,
		Background
	}

	internal enum ApiMetricsOperation
	{
		Read,
		Write,
		Batch,
		Query,
		CountOnly,
		Uninstrumented
	}

	internal enum MetricsAttemptMode
	{
		Operational,
		CompatibilityOnly
	}

	internal sealed class MetricsCallState
	{
		private readonly MetricsStore store;
		private readonly ApiMetricsOperation operation;
		private readonly long startTimestamp;
		private readonly long sessionId;
		private readonly bool sampled;
		private int completed;
		private int iteratorOwned;
		private int asyncOwned;
		private int successfulWork;

		internal MetricsCallState(
			MetricsStore store,
			ApiMetricsOperation operation,
			bool sampled)
		{
			this.store = store;
			this.operation = operation;
			this.sampled = sampled;
			sessionId = store.SessionId;
			startTimestamp = sampled && operation != ApiMetricsOperation.CountOnly
				? store.Clock.GetTimestamp()
				: 0;
		}

		internal void TransferToIterator()
		{
			Volatile.Write(ref iteratorOwned, 1);
		}

		internal void TransferToAsync()
		{
			Volatile.Write(ref asyncOwned, 1);
		}

		internal void MarkSuccessfulWork()
		{
			Volatile.Write(ref successfulWork, 1);
		}

		internal void CompleteSuccess()
		{
			if (Interlocked.CompareExchange(ref completed, 1, 0) == 0 &&
				startTimestamp != 0)
			{
				store.RecordCallSuccess(
					operation,
					startTimestamp,
					sessionId);
			}
		}

		internal void CompleteFailure()
		{
			Interlocked.CompareExchange(ref completed, 2, 0);
		}

		internal void CompleteAbandoned()
		{
			if (Interlocked.CompareExchange(ref completed, 3, 0) == 0 &&
				sampled &&
				operation == ApiMetricsOperation.Query)
			{
				store.AddQueryAbandoned();
			}
		}

		internal void CompleteScope()
		{
			if (Volatile.Read(ref iteratorOwned) != 0 ||
				Volatile.Read(ref asyncOwned) != 0)
			{
				return;
			}

			if (Volatile.Read(ref successfulWork) != 0)
			{
				CompleteSuccess();
			}
			else
			{
				CompleteFailure();
			}
		}
	}

	internal readonly struct MetricsCallContext
	{
		private readonly Cluster cluster;
		private readonly MetricsStore store;
		private readonly MetricsCallState callState;
		private readonly CommandCountMetricsHandle commandCountMetrics;
		private readonly ApiMetricsOperation operation;
		private readonly ApiExecutionMode executionMode;
		private readonly bool sampled;

		internal MetricsCallContext(
			Cluster cluster,
			MetricsStore store,
			ApiMetricsOperation operation,
			ApiExecutionMode executionMode,
			bool sampled,
			bool createCallState = true)
		{
			this.cluster = cluster;
			this.store = store;
			this.operation = operation;
			this.executionMode = executionMode;
			this.sampled = sampled;
			callState = store != null && createCallState
				? new MetricsCallState(store, operation, sampled)
				: null;
			commandCountMetrics = new CommandCountMetricsHandle(store, sampled);
		}

		internal bool IsFor(Cluster owner) => ReferenceEquals(cluster, owner);

		internal CommandCountMetricsHandle CommandCountMetrics => commandCountMetrics;
		internal bool CompatibilityOnly => operation == ApiMetricsOperation.CountOnly;
		internal bool CommandOwnsCall =>
			executionMode == ApiExecutionMode.Deferred &&
			(operation == ApiMetricsOperation.Read ||
				operation == ApiMetricsOperation.Write);

		internal CommandMetricsHandle BeginCommand(
			string ns,
			IReadOnlyList<string> namespaces,
			LatencyType latencyType,
			bool countCommand)
		{
			MetricsAttemptMode mode = operation == ApiMetricsOperation.CountOnly
				? MetricsAttemptMode.CompatibilityOnly
				: MetricsAttemptMode.Operational;
			return new CommandMetricsHandle(
				new CommandMetricsState(
					store,
					ns,
					namespaces,
					latencyType,
					sampled,
					mode));
		}

		internal CommandMetricsHandle BeginCommand(
			string ns,
			LatencyType latencyType,
			bool countCommand)
		{
			return BeginCommand(ns, null, latencyType, countCommand);
		}

		internal QueryMetricsHandle BeginQueryMetrics()
		{
			return sampled && operation == ApiMetricsOperation.Query && store != null
				? new QueryMetricsHandle(store)
				: default;
		}

		internal MetricsRpcAttempt BeginCompatibilityAttempt()
		{
			return CompatibilityOnly && store != null
				? new MetricsRpcAttempt(
					store,
					null,
					Array.Empty<string>(),
					null,
					LatencyType.NONE,
					sampled,
					MetricsAttemptMode.CompatibilityOnly)
				: null;
		}

		internal void TransferToIterator()
		{
			callState?.TransferToIterator();
		}

		internal void TransferToAsync()
		{
			callState?.TransferToAsync();
		}

		internal void MarkSuccessfulWork()
		{
			callState?.MarkSuccessfulWork();
		}

		internal void CompleteSuccess()
		{
			callState?.CompleteSuccess();
		}

		internal void CompleteFailure()
		{
			callState?.CompleteFailure();
		}

		internal void CompleteAbandoned()
		{
			callState?.CompleteAbandoned();
		}

		internal void CompleteScope()
		{
			callState?.CompleteScope();
		}
	}

	internal struct ApiMetricsCall : IDisposable
	{
		private readonly Cluster cluster;
		private readonly MetricsCallContext previous;
		private readonly bool previousDelegation;
		private readonly MetricsCallContext owned;

		internal ApiMetricsCall(
			Cluster cluster,
			MetricsCallContext previous,
			bool previousDelegation,
			MetricsCallContext owned)
		{
			this.cluster = cluster;
			this.previous = previous;
			this.previousDelegation = previousDelegation;
			this.owned = owned;
		}

		internal void TransferToIterator()
		{
			owned.TransferToIterator();
		}

		public void Dispose()
		{
			cluster?.EndApiMetricsCall(
				previous,
				previousDelegation,
				owned);
		}
	}

	internal sealed class CommandMetricsState
	{
		internal readonly MetricsStore Store;
		internal readonly string Namespace;
		internal readonly IReadOnlyList<string> Namespaces;
		internal readonly string BytesNamespace;
		internal readonly LatencyType LatencyType;
		internal readonly bool Sampled;
		internal readonly MetricsAttemptMode Mode;
		private MetricsRpcAttempt legacyAttempt;
		private readonly object legacyLock = new();
		private int apiOwner;

		internal CommandMetricsState(
			MetricsStore store,
			string ns,
			IReadOnlyList<string> namespaces,
			LatencyType latencyType,
			bool sampled,
			MetricsAttemptMode mode,
			bool apiOwned = false)
		{
			Store = store;
			Namespace = ns;
			Namespaces = NormalizeNamespaces(ns, namespaces);
			BytesNamespace = Namespaces.Count > 1 ? string.Empty : ns;
			LatencyType = latencyType;
			Sampled = sampled;
			Mode = mode;
			apiOwner = apiOwned ? 1 : 0;
		}

		internal bool TransferApiOwner()
		{
			return Interlocked.CompareExchange(ref apiOwner, 2, 1) == 1;
		}

		internal void AbandonApiOwner()
		{
			Interlocked.CompareExchange(ref apiOwner, 3, 1);
		}

		internal MetricsRpcAttempt BeginAttempt()
		{
			return new MetricsRpcAttempt(
				Store,
				Namespace,
				Namespaces,
				BytesNamespace,
				LatencyType,
				Sampled,
				Mode);
		}

		internal void AddRetry()
		{
			if (Sampled)
			{
				Store?.AddCommandRetry();
			}
		}

		internal CommandMetricsState ForkRpc(
			string childNamespace = null,
			IReadOnlyList<string> childNamespaces = null)
		{
			return new CommandMetricsState(
				Store,
				childNamespace ?? Namespace,
				childNamespaces ?? Namespaces,
				LatencyType,
				Sampled,
				Mode);
		}

		private static IReadOnlyList<string> NormalizeNamespaces(
			string ns,
			IReadOnlyList<string> namespaces)
		{
			if (namespaces != null && namespaces.Count != 0)
			{
				return Array.AsReadOnly(
					namespaces
						.Select(value => value ?? string.Empty)
						.Distinct(StringComparer.Ordinal)
						.ToArray());
			}
			return ns == null
				? Array.Empty<string>()
				: Array.AsReadOnly([ns]);
		}

		internal MetricsRpcAttempt GetLegacyAttempt()
		{
			lock (legacyLock)
			{
				if (legacyAttempt == null)
				{
					legacyAttempt = BeginAttempt();
				}
				return legacyAttempt;
			}
		}
	}

	internal sealed class MetricsRpcAttempt
	{
		private readonly MetricsStore store;
		private readonly IReadOnlyList<string> namespaces;
		private readonly string bytesNamespace;
		private readonly LatencyType latencyType;
		private readonly bool sampled;
		private readonly MetricsAttemptMode mode;
		private readonly long startTimestamp;
		private readonly long sessionId;
		private long bytesIn;
		private long bytesOut;
		private int terminal;
		private readonly object terminalLock = new();

		internal MetricsRpcAttempt(
			MetricsStore store,
			string ns,
			IReadOnlyList<string> namespaces,
			string bytesNamespace,
			LatencyType latencyType,
			bool sampled,
			MetricsAttemptMode mode)
		{
			this.store = store;
			this.namespaces = namespaces;
			this.bytesNamespace = bytesNamespace;
			this.latencyType = latencyType;
			this.sampled = sampled;
			this.mode = mode;
			sessionId = store?.SessionId ?? 0;
			startTimestamp = sampled &&
				mode == MetricsAttemptMode.Operational &&
				store != null
					? store.Clock.GetTimestamp()
					: 0;
		}

		internal void AddBytes(long inCount, long outCount)
		{
			Interlocked.Add(ref bytesIn, inCount);
			Interlocked.Add(ref bytesOut, outCount);
		}

		internal bool TrySucceed(Node node)
		{
			lock (terminalLock)
			{
				if (terminal != 0)
				{
					return false;
				}
				terminal = 1;

				if (mode == MetricsAttemptMode.Operational &&
					store != null &&
					node != null &&
					store.IsCurrentSession(sessionId))
				{
					long elapsed = startTimestamp == 0
						? 0
						: store.Clock.GetElapsedNanoseconds(startTimestamp);
					store.RecordAttemptSuccess(
						node,
						namespaces,
						bytesNamespace,
						latencyType,
						elapsed,
						Volatile.Read(ref bytesIn),
						Volatile.Read(ref bytesOut),
						sampled,
						sessionId);
				}
				return true;
			}
		}

		internal bool TryFail(
			Node node,
			AerospikeException exception,
			bool clientTimeout)
		{
			if (exception?.Result == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return TrySucceed(node);
			}

			lock (terminalLock)
			{
				if (terminal != 0)
				{
					return false;
				}
				terminal = 2;

				store?.RecordCompatibilityFailure(
					node,
					bytesNamespace,
					exception,
					clientTimeout);

				if (store != null && !store.IsCurrentSession(sessionId))
				{
					return true;
				}

				MetricsFailure failure = Classify(exception);

				if (sampled &&
					mode == MetricsAttemptMode.Operational &&
					store != null &&
					node != null &&
					namespaces.Count != 0 &&
					failure != MetricsFailure.None)
				{
					store.RecordAttemptFailure(
						node,
						namespaces,
						failure,
						clientTimeout,
						sessionId);
				}
				return true;
			}
		}

		internal void RecordBatchRecordFailure(
			Node node,
			string ns,
			int resultCode)
		{
			lock (terminalLock)
			{
				if (terminal != 0 ||
					!sampled ||
					mode != MetricsAttemptMode.Operational ||
					store == null ||
					node == null)
				{
					return;
				}

				MetricsFailure failure = ClassifyResult(resultCode);

				if (failure != MetricsFailure.None)
				{
					store.RecordBatchRecordFailure(
						node,
						ns,
						failure,
						sessionId);
				}
			}
		}

		private static MetricsFailure Classify(AerospikeException exception)
		{
			if (exception == null)
			{
				return MetricsFailure.Error;
			}

			return ClassifyResult(exception.Result);
		}

		private static MetricsFailure ClassifyResult(int resultCode)
		{
			return resultCode switch
			{
				ResultCode.OK => MetricsFailure.None,
				ResultCode.KEY_NOT_FOUND_ERROR => MetricsFailure.None,
				ResultCode.TIMEOUT => MetricsFailure.Timeout,
				ResultCode.KEY_BUSY => MetricsFailure.KeyBusy,
				ResultCode.RECORD_TOO_BIG => MetricsFailure.RecordTooBig,
				ResultCode.DEVICE_OVERLOAD => MetricsFailure.DeviceOverload,
				ResultCode.INVALID_NODE_ERROR => MetricsFailure.None,
				ResultCode.MAX_ERROR_RATE => MetricsFailure.None,
				ResultCode.NO_MORE_CONNECTIONS => MetricsFailure.None,
				_ => MetricsFailure.Error
			};
		}
	}

	internal readonly struct CommandMetricsHandle
	{
		private readonly CommandMetricsState state;

		internal bool Enabled =>
			state != null &&
			state.Sampled &&
			state.Mode == MetricsAttemptMode.Operational;

		internal CommandMetricsHandle(CommandMetricsState state)
		{
			this.state = state;
		}

		internal void AddBytes(Node node, long bytesIn, long bytesOut)
		{
			state?.GetLegacyAttempt().AddBytes(bytesIn, bytesOut);
		}

		internal void AddRetry()
		{
			state?.AddRetry();
		}

		internal CommandMetricsHandle ForkRpc(
			string childNamespace = null,
			IReadOnlyList<string> childNamespaces = null)
		{
			return state == null
				? default
				: new CommandMetricsHandle(
					state.ForkRpc(childNamespace, childNamespaces));
		}

		internal CommandMetricsHandle TransferApiOwner()
		{
			return state?.TransferApiOwner() == true ? this : default;
		}

		internal void AbandonApiOwner()
		{
			state?.AbandonApiOwner();
		}

		internal void CompleteSuccess(Node node)
		{
			state?.GetLegacyAttempt().TrySucceed(node);
		}

		internal void CompleteFailure(Node node, AerospikeException exception, bool clientTimeout)
		{
			state?.GetLegacyAttempt().TryFail(node, exception, clientTimeout);
		}

		internal MetricsRpcAttempt BeginAttempt()
		{
			return state?.BeginAttempt();
		}

		internal void RecordBatchRecordFailure(
			Node node,
			string ns,
			int resultCode)
		{
			state?.GetLegacyAttempt().RecordBatchRecordFailure(
				node,
				ns,
				resultCode);
		}
	}

	internal readonly struct QueryMetricsHandle
	{
		private readonly MetricsStore store;

		internal QueryMetricsHandle(MetricsStore store)
		{
			this.store = store;
		}

		internal bool Enabled => store != null;

		internal void AddQueued(int delta)
		{
			store?.AddQueryRecordsQueued(delta);
		}

		internal void AddServer(int delta)
		{
			store?.AddQueryServersConcurrent(delta);
		}

		internal void AddAbandoned()
		{
			store?.AddQueryAbandoned();
		}
	}

	internal readonly struct CommandCountMetricsHandle
	{
		private readonly MetricsStore store;
		private readonly bool sampled;

		internal CommandCountMetricsHandle(MetricsStore store, bool sampled = true)
		{
			this.store = store;
			this.sampled = sampled;
		}

		internal MetricsStore Store => store;
		internal bool Sampled => sampled;

		internal void AddBatchPartialFailure()
		{
			if (sampled)
			{
				store?.AddBatchPartialFailure();
			}
		}
	}
}
