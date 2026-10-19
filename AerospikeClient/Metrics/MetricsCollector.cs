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

	internal sealed class MetricsHistogramShape
	{
		internal readonly long Generation;
		internal readonly MetricsLatencyUnit Unit;
		internal readonly int Columns;
		internal readonly int Shift;
		internal readonly ulong[] Limits;

		internal MetricsHistogramShape(
			long generation,
			MetricsLatencyUnit unit,
			int columns,
			int shift)
		{
			Generation = generation;
			Unit = unit;
			Columns = columns;
			Shift = shift;
			Limits = CreateLimits(columns, shift);
		}

		internal bool Matches(MetricsPolicy policy)
		{
			return Unit == policy.LatencyUnit &&
				Columns == policy.LatencyColumns &&
				Shift == policy.LatencyShift;
		}

		private static ulong[] CreateLimits(int columns, int shift)
		{
			ulong[] result = new ulong[columns];
			ulong limit = 1;

			for (int i = 0; i < columns; i++)
			{
				result[i] = limit;

				if (shift >= 64 || limit > (ulong.MaxValue >> shift))
				{
					limit = ulong.MaxValue;
				}
				else
				{
					limit <<= shift;
				}
			}
			return result;
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

	internal sealed class MetricsHistogram
	{
		private readonly long[] buckets;
		private readonly ulong[] limits;
		private readonly MetricsLatencyUnit unit;

		internal MetricsHistogram(MetricsHistogramShape shape)
		{
			buckets = new long[shape.Columns];
			limits = shape.Limits;
			unit = shape.Unit;
		}

		internal void AddNanoseconds(long elapsedNanoseconds)
		{
			ulong divisor = unit == MetricsLatencyUnit.Microseconds ? 1_000UL : 1_000_000UL;
			ulong value = elapsedNanoseconds <= 0 ? 0 : (ulong)elapsedNanoseconds / divisor;
			int index = limits.Length - 1;

			for (int i = 0; i < limits.Length - 1; i++)
			{
				if (value <= limits[i])
				{
					index = i;
					break;
				}
			}
			Interlocked.Increment(ref buckets[index]);
		}

		internal long[] CopyBuckets()
		{
			long[] copy = new long[buckets.Length];

			for (int i = 0; i < buckets.Length; i++)
			{
				copy[i] = Volatile.Read(ref buckets[i]);
			}
			return copy;
		}

		internal void AddBuckets(IReadOnlyList<long> source)
		{
			int count = Math.Min(buckets.Length, source.Count);

			for (int i = 0; i < count; i++)
			{
				Interlocked.Add(ref buckets[i], source[i]);
			}
		}

	}

	internal sealed class MetricsNamespaceCollector
	{
		private sealed class HistogramBank
		{
			internal readonly long ShapeGeneration;
			internal readonly MetricsHistogram[] Histograms;

			internal HistogramBank(MetricsHistogramShape shape)
			{
				ShapeGeneration = shape.Generation;
				Histograms = new MetricsHistogram[Latency.GetMax()];

				for (int i = 0; i < Histograms.Length; i++)
				{
					Histograms[i] = new MetricsHistogram(shape);
				}
			}
		}

		private long errors;
		private long timeouts;
		private long keyBusy;
		private long recordTooBig;
		private long deviceOverload;
		private long bytesIn;
		private long bytesOut;
		private volatile HistogramBank histogramBank;
		private readonly object configLock = new();

		internal MetricsNamespaceCollector(MetricsHistogramShape shape)
		{
			histogramBank = new HistogramBank(shape);
		}

		internal void AddBytes(long inCount, long outCount)
		{
			Interlocked.Add(ref bytesIn, inCount);
			Interlocked.Add(ref bytesOut, outCount);
		}

		internal void AddLatency(
			LatencyType type,
			long elapsedNanoseconds,
			MetricsHistogramShape expected)
		{
			if (type != LatencyType.NONE)
			{
				HistogramBank current = EnsureShapeBank(expected);
				current.Histograms[(int)type].AddNanoseconds(elapsedNanoseconds);
			}
		}

		internal void EnsureShape(MetricsHistogramShape shape)
		{
			EnsureShapeBank(shape);
		}

		private HistogramBank EnsureShapeBank(MetricsHistogramShape shape)
		{
			HistogramBank current = histogramBank;

			if (current.ShapeGeneration >= shape.Generation)
			{
				return current;
			}

			lock (configLock)
			{
				current = histogramBank;

				if (current.ShapeGeneration < shape.Generation)
				{
					current = new HistogramBank(shape);
					histogramBank = current;
				}
				return current;
			}
		}

		internal void AddFailure(MetricsFailure failure)
		{
			switch (failure)
			{
				case MetricsFailure.Timeout:
					Interlocked.Increment(ref timeouts);
					break;

				case MetricsFailure.KeyBusy:
					Interlocked.Increment(ref keyBusy);
					break;

				case MetricsFailure.RecordTooBig:
					Interlocked.Increment(ref recordTooBig);
					break;

				case MetricsFailure.DeviceOverload:
					Interlocked.Increment(ref deviceOverload);
					break;

				case MetricsFailure.Error:
					Interlocked.Increment(ref errors);
					break;
			}
		}

		internal MetricsNamespaceView Read(string ns, MetricsRuntimeConfig config)
		{
			Dictionary<LatencyType, long[]> latency = new();
			HistogramBank bank = EnsureShapeBank(config.Shape);
			MetricsHistogram[] current = bank.Histograms;

			for (int i = 0; i < current.Length; i++)
			{
				latency[(LatencyType)i] = current[i].CopyBuckets();
			}

			Dictionary<LatencyType, IReadOnlyList<long>> immutableLatency = new(latency.Count);

			foreach (KeyValuePair<LatencyType, long[]> entry in latency)
			{
				immutableLatency[entry.Key] = Array.AsReadOnly(entry.Value);
			}

			return new MetricsNamespaceView(
				ns,
				Volatile.Read(ref errors),
				Volatile.Read(ref timeouts),
				Volatile.Read(ref keyBusy),
				Volatile.Read(ref recordTooBig),
				Volatile.Read(ref deviceOverload),
				Volatile.Read(ref bytesIn),
				Volatile.Read(ref bytesOut),
				new ReadOnlyDictionary<LatencyType, IReadOnlyList<long>>(immutableLatency));
		}

		internal void Merge(MetricsNamespaceView source, MetricsHistogramShape expected)
		{
			Interlocked.Add(ref errors, source.Errors);
			Interlocked.Add(ref timeouts, source.Timeouts);
			Interlocked.Add(ref keyBusy, source.KeyBusy);
			Interlocked.Add(ref recordTooBig, source.RecordTooBig);
			Interlocked.Add(ref deviceOverload, source.DeviceOverload);
			Interlocked.Add(ref bytesIn, source.BytesIn);
			Interlocked.Add(ref bytesOut, source.BytesOut);

			HistogramBank bank = EnsureShapeBank(expected);

			foreach (KeyValuePair<LatencyType, IReadOnlyList<long>> entry in source.Latency)
			{
				bank.Histograms[(int)entry.Key].AddBuckets(entry.Value);
			}
		}
	}

	internal sealed class MetricsNodeCollector
	{
		private readonly ConcurrentDictionary<string, MetricsNamespaceCollector> namespaces = new();
		private volatile MetricsHistogramShape shape;
		private long opened;
		private long closed;
		private long poolExhausted;
		private long closedError;
		private long closedIdle;
		private long closedPoolShrink;
		private long closedNodeRemoved;
		private readonly object configLock = new();

		internal MetricsNodeCollector(MetricsRuntimeConfig config)
		{
			shape = config.Shape;
		}

		internal MetricsNamespaceCollector GetNamespace(
			string ns,
			MetricsHistogramShape expected)
		{
			MetricsHistogramShape current = shape;
			MetricsHistogramShape target =
				current.Generation >= expected.Generation ? current : expected;
			MetricsNamespaceCollector collector = namespaces.GetOrAdd(
				ns ?? string.Empty,
				_ => new MetricsNamespaceCollector(target));
			collector.EnsureShape(target);
			return collector;
		}

		internal void EnsureShape(MetricsHistogramShape replacement)
		{
			if (shape.Generation >= replacement.Generation)
			{
				return;
			}

			lock (configLock)
			{
				if (shape.Generation >= replacement.Generation)
				{
					return;
				}

				// Publish the cutover before enumeration. A namespace inserted before
				// this store is included below; one inserted afterwards observes the
				// replacement shape in GetNamespace().
				shape = replacement;
				foreach (MetricsNamespaceCollector collector in namespaces.Values)
				{
					collector.EnsureShape(replacement);
				}
			}
		}

		internal void AddOpened()
		{
			Interlocked.Increment(ref opened);
		}

		internal void AddClosed(
			ConnectionCloseReason reason,
			bool operationalEnabled)
		{
			Interlocked.Increment(ref closed);

			if (!operationalEnabled)
			{
				return;
			}

			switch (reason)
			{
				case ConnectionCloseReason.Error:
					Interlocked.Increment(ref closedError);
					break;

				case ConnectionCloseReason.Idle:
					Interlocked.Increment(ref closedIdle);
					break;

				case ConnectionCloseReason.PoolShrink:
					Interlocked.Increment(ref closedPoolShrink);
					break;

				case ConnectionCloseReason.NodeRemoved:
					Interlocked.Increment(ref closedNodeRemoved);
					break;
			}
		}

		internal void AddPoolExhausted()
		{
			Interlocked.Increment(ref poolExhausted);
		}

		internal MetricsNodeView Read(string name, MetricsRuntimeConfig expected)
		{
			EnsureShape(expected.Shape);
			Dictionary<string, MetricsNamespaceView> namespaceViews = new();

			foreach (KeyValuePair<string, MetricsNamespaceCollector> entry in namespaces)
			{
				namespaceViews[entry.Key] = entry.Value.Read(entry.Key, expected);
			}

			return new MetricsNodeView(
				name,
				Volatile.Read(ref opened),
				Volatile.Read(ref closed),
				Volatile.Read(ref poolExhausted),
				Volatile.Read(ref closedError),
				Volatile.Read(ref closedIdle),
				Volatile.Read(ref closedPoolShrink),
				Volatile.Read(ref closedNodeRemoved),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(namespaceViews));
		}

		internal void Merge(MetricsNodeView source, MetricsRuntimeConfig expected)
		{
			Interlocked.Add(ref opened, source.Opened);
			Interlocked.Add(ref closed, source.Closed);
			Interlocked.Add(ref poolExhausted, source.PoolExhausted);
			Interlocked.Add(ref closedError, source.ClosedError);
			Interlocked.Add(ref closedIdle, source.ClosedIdle);
			Interlocked.Add(ref closedPoolShrink, source.ClosedPoolShrink);
			Interlocked.Add(ref closedNodeRemoved, source.ClosedNodeRemoved);

			foreach (KeyValuePair<string, MetricsNamespaceView> entry in source.Namespaces)
			{
				GetNamespace(entry.Key, expected.Shape).Merge(entry.Value, expected.Shape);
			}
		}
	}

	internal enum MetricsFailure
	{
		None,
		Timeout,
		KeyBusy,
		RecordTooBig,
		DeviceOverload,
		Error
	}

	internal enum ConnectionCloseReason
	{
		Unknown,
		Error,
		Idle,
		PoolShrink,
		NodeRemoved
	}

	internal enum ApiExecutionMode
	{
		Blocking,
		Deferred,
		Background
	}

	internal readonly struct ApiMetricsContext
	{
		private readonly Cluster cluster;
		private readonly MetricsSession session;
		private readonly MetricsRuntimeConfig runtimeConfig;
		private readonly CommandMetricsHandle pointMetrics;
		private readonly CommandCountMetricsHandle commandCountMetrics;
		private readonly bool pointCall;
		private readonly bool inheritCoordinatorSampling;

		internal ApiMetricsContext(
			Cluster cluster,
			MetricsSession session,
			MetricsRuntimeConfig runtimeConfig,
			CommandMetricsHandle pointMetrics)
		{
			this.cluster = cluster;
			this.session = session;
			this.runtimeConfig = runtimeConfig;
			this.pointMetrics = pointMetrics;
			commandCountMetrics = default;
			pointCall = true;
			inheritCoordinatorSampling = false;
		}

		internal ApiMetricsContext(
			Cluster cluster,
			MetricsSession session,
			MetricsRuntimeConfig runtimeConfig,
			CommandCountMetricsHandle commandCountMetrics,
			bool inheritCoordinatorSampling)
		{
			this.cluster = cluster;
			this.session = session;
			this.runtimeConfig = runtimeConfig;
			pointMetrics = default;
			this.commandCountMetrics = commandCountMetrics;
			pointCall = false;
			this.inheritCoordinatorSampling = inheritCoordinatorSampling;
		}

		internal bool IsFor(Cluster owner) => ReferenceEquals(cluster, owner);

		internal CommandCountMetricsHandle CommandCountMetrics => commandCountMetrics;

		internal CommandMetricsHandle BeginCommand(
			string ns,
			LatencyType latencyType,
			bool countCommand)
		{
			if (pointCall)
			{
				return pointMetrics.TransferApiOwner();
			}

			if (session == null)
			{
				return default;
			}

			if (inheritCoordinatorSampling)
			{
				return commandCountMetrics.Sampled
					? session.BeginCapturedSampledCommand(ns, latencyType)
					: default;
			}
			return session.BeginCapturedCommand(ns, latencyType, runtimeConfig);
		}

		internal QueryMetricsHandle BeginQueryMetrics()
		{
			return session != null
				? new QueryMetricsHandle(session)
				: default;
		}

		internal void AbandonPointOwner()
		{
			if (pointCall)
			{
				pointMetrics.AbandonApiOwner();
			}
		}
	}

	internal readonly struct ApiMetricsCall : IDisposable
	{
		private readonly Cluster cluster;
		private readonly ApiMetricsContext previous;
		private readonly bool previousDelegation;
		private readonly ApiMetricsContext owned;
		private readonly bool abandonPoint;

		internal ApiMetricsCall(
			Cluster cluster,
			ApiMetricsContext previous,
			bool previousDelegation,
			ApiMetricsContext owned,
			bool abandonPoint)
		{
			this.cluster = cluster;
			this.previous = previous;
			this.previousDelegation = previousDelegation;
			this.owned = owned;
			this.abandonPoint = abandonPoint;
		}

		public void Dispose()
		{
			cluster?.EndApiMetricsCall(
				previous,
				previousDelegation,
				owned,
				abandonPoint);
		}
	}

	internal sealed class CommandMetricsState
	{
		private sealed class ByteCounts
		{
			internal long BytesIn;
			internal long BytesOut;
		}

		private readonly MetricsSession session;
		private readonly string ns;
		private readonly LatencyType latencyType;
		private readonly long startTimestamp;
		private readonly Dictionary<Node, ByteCounts> bytesByNode = new();
		private readonly object bytesLock = new();
		private int completed;
		private int apiOwner;

		internal CommandMetricsState(
			MetricsSession session,
			string ns,
			LatencyType latencyType,
			bool apiOwned = false)
		{
			this.session = session;
			this.ns = ns;
			this.latencyType = latencyType;
			apiOwner = apiOwned ? 1 : 0;
			startTimestamp = session.Clock.GetTimestamp();
		}

		internal bool TransferApiOwner()
		{
			return Interlocked.CompareExchange(ref apiOwner, 2, 1) == 1;
		}

		internal void AbandonApiOwner()
		{
			if (Interlocked.CompareExchange(ref apiOwner, 3, 1) == 1)
			{
				Interlocked.CompareExchange(ref completed, 1, 0);
			}
		}

		internal void AddBytes(Node node, long bytesIn, long bytesOut)
		{
			if (node == null || (bytesIn == 0 && bytesOut == 0))
			{
				return;
			}

			lock (bytesLock)
			{
				if (!bytesByNode.TryGetValue(node, out ByteCounts counts))
				{
					counts = new ByteCounts();
					bytesByNode[node] = counts;
				}
				counts.BytesIn += bytesIn;
				counts.BytesOut += bytesOut;
			}
		}

		internal void AddRetry()
		{
			session.AddCommandRetry();
		}

		internal CommandMetricsState ForkRpc(string childNamespace = null)
		{
			using MetricsWritePermit permit = session.AcquireWrite();
			return permit.Acquired
				? new CommandMetricsState(
					session,
					childNamespace ?? ns,
					latencyType)
				: null;
		}

		internal void CompleteSuccess(Node node)
		{
			using MetricsWritePermit permit = session.AcquireWrite();

			if (!permit.Acquired ||
				Interlocked.CompareExchange(ref completed, 1, 0) != 0)
			{
				return;
			}

			if (ns != null)
			{
				lock (bytesLock)
				{
					foreach (KeyValuePair<Node, ByteCounts> entry in bytesByNode)
					{
						session.AddCapturedBytesAdmitted(
							entry.Key,
							ns,
							entry.Value.BytesIn,
							entry.Value.BytesOut);
					}
				}

				if (node != null)
				{
					long elapsed = session.Clock.GetElapsedNanoseconds(startTimestamp);
					session.AddCapturedLatencyAdmitted(node, ns, latencyType, elapsed);
				}
			}
		}

		internal void CompleteFailure(Node node, MetricsFailure failure, bool clientTimeout)
		{
			using MetricsWritePermit permit = session.AcquireWrite();

			if (!permit.Acquired ||
				Interlocked.CompareExchange(ref completed, 1, 0) != 0)
			{
				return;
			}

			if (failure == MetricsFailure.Timeout)
			{
				session.AddTimeoutAdmitted(clientTimeout);
			}

			if (node != null && ns != null && failure != MetricsFailure.None)
			{
				session.AddCapturedFailureAdmitted(node, ns, failure);
			}
		}
	}

	internal readonly struct CommandMetricsHandle
	{
		private readonly CommandMetricsState state;

		internal bool Enabled => state != null;

		internal CommandMetricsHandle(CommandMetricsState state)
		{
			this.state = state;
		}

		internal void AddBytes(Node node, long bytesIn, long bytesOut)
		{
			state?.AddBytes(node, bytesIn, bytesOut);
		}

		internal void AddRetry()
		{
			state?.AddRetry();
		}

		internal CommandMetricsHandle ForkRpc(string childNamespace = null)
		{
			return state == null
				? default
				: new CommandMetricsHandle(state.ForkRpc(childNamespace));
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
			state?.CompleteSuccess(node);
		}

		internal void CompleteFailure(Node node, AerospikeException exception, bool clientTimeout)
		{
			if (exception?.Result == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				state?.CompleteSuccess(node);
			}
			else
			{
				state?.CompleteFailure(node, Classify(exception), clientTimeout);
			}
		}

		private static MetricsFailure Classify(AerospikeException exception)
		{
			if (exception == null)
			{
				return MetricsFailure.Error;
			}

			return exception.Result switch
			{
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

	internal readonly struct QueryMetricsHandle
	{
		private readonly MetricsSession session;

		internal QueryMetricsHandle(MetricsSession session)
		{
			this.session = session;
		}

		internal bool Enabled => session != null;

		internal void AddQueued(int delta)
		{
			session?.AddQueryRecordsQueued(delta);
		}

		internal void AddServer(int delta)
		{
			session?.AddQueryServersConcurrent(delta);
		}

		internal void AddAbandoned()
		{
			session?.AddQueryAbandoned();
		}
	}

	internal readonly struct CommandCountMetricsHandle
	{
		private readonly MetricsSession session;
		private readonly bool sampled;

		internal CommandCountMetricsHandle(MetricsSession session, bool sampled = true)
		{
			this.session = session;
			this.sampled = sampled;
		}

		internal MetricsSession Session => session;
		internal bool Sampled => sampled;

		internal void AddBatchPartialFailure()
		{
			if (sampled)
			{
				session?.AddBatchPartialFailure();
			}
		}
	}

	internal sealed class ConnectionMetricsState
	{
		private readonly MetricsSession session;
		private readonly MetricsRuntimeConfig runtimeConfig;
		private readonly long startTimestamp;
		private int state;

		private const int TerminalMask = 3;
		private const int Succeeded = 1;
		private const int Failed = 2;
		private const int Canceled = 3;
		private const int TlsRecorded = 4;
		private const int AuthRecorded = 8;

		internal ConnectionMetricsState(
			MetricsSession session,
			MetricsRuntimeConfig runtimeConfig,
			long startTimestamp)
		{
			this.session = session;
			this.runtimeConfig = runtimeConfig;
			this.startTimestamp = startTimestamp;
		}

		internal void Complete(Node node, string ns)
		{
			using MetricsWritePermit permit = session.AcquireWrite();

			if (permit.Acquired &&
				TrySetTerminal(Succeeded))
			{
				session.AddConnectionOpenedAdmitted(node);
				session.CompleteConnectionAdmitted(
					node,
					ns,
					startTimestamp,
					runtimeConfig);
			}
		}

		internal void Fail(ConnectionFailurePhase phase)
		{
			using MetricsWritePermit permit = session.AcquireWrite();

			if (permit.Acquired && TrySetTerminal(Failed))
			{
				if (runtimeConfig.OperationalEnabled)
				{
					session.AddCapturedConnectionFailureAdmitted(
						ConnectionFailurePhase.Open,
						true);
					RecordPhase(phase);
				}
			}
		}

		internal void FailPhase(ConnectionFailurePhase phase)
		{
			using MetricsWritePermit permit = session.AcquireWrite();

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
					session.AddCapturedConnectionFailureAdmitted(phase, false);
					return;
				}
			}
		}
	}

	internal readonly struct ConnectionMetricsHandle
	{
		private readonly ConnectionMetricsState state;

		internal bool Enabled => state != null;

		internal ConnectionMetricsHandle(
			MetricsSession session,
			long startTimestamp)
			: this(session, session?.RuntimeConfig, startTimestamp)
		{
		}

		internal ConnectionMetricsHandle(
			MetricsSession session,
			MetricsRuntimeConfig runtimeConfig,
			long startTimestamp)
		{
			state = session == null
				? null
				: new ConnectionMetricsState(session, runtimeConfig, startTimestamp);
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
		private readonly MetricsSession session;
		private int state;

		internal RecoverMetricsState(MetricsSession session)
		{
			this.session = session;
		}

		internal void Enqueue()
		{
			using MetricsWritePermit permit = session.AcquireWrite();

			if (permit.Acquired &&
				Interlocked.CompareExchange(ref state, 1, 0) == 0)
			{
				session.AddCapturedRecoverEnqueuedAdmitted();
			}
		}

		internal void Complete()
		{
			using MetricsWritePermit permit = session.AcquireWrite();

			if (permit.Acquired &&
				Interlocked.CompareExchange(ref state, 2, 1) == 1)
			{
				session.AddCapturedRecoverCompletedAdmitted();
			}
		}

		internal void Abort()
		{
			using MetricsWritePermit permit = session.AcquireWrite();

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
						session.AddCapturedRecoverAbortedAdmitted();
						return;
					}
				}
			}
		}
	}

	internal readonly struct RecoverMetricsHandle
	{
		private readonly RecoverMetricsState state;

		internal RecoverMetricsHandle(MetricsSession session)
		{
			state = session == null ? null : new RecoverMetricsState(session);
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

	internal readonly struct MetricsWritePermit : IDisposable
	{
		private readonly MetricsSession session;

		internal MetricsWritePermit(MetricsSession session)
		{
			this.session = session;
		}

		internal bool Acquired => session != null;

		public void Dispose()
		{
			session?.ReleaseWrite();
		}
	}

	internal readonly struct MetricsBeginPermit : IDisposable
	{
		private readonly MetricsSession session;

		internal MetricsBeginPermit(MetricsSession session)
		{
			this.session = session;
		}

		internal bool Acquired => session != null;

		public void Dispose()
		{
			session?.ReleaseBegin();
		}
	}

	internal sealed class MetricsSession
	{
		private enum Lifecycle
		{
			Frozen,
			Active,
			Paused
		}

		private sealed class HistogramState
		{
			internal readonly long ShapeGeneration;
			internal readonly MetricsHistogram Histogram;

			internal HistogramState(MetricsHistogramShape shape)
			{
				ShapeGeneration = shape.Generation;
				Histogram = new MetricsHistogram(shape);
			}
		}

		private static long nextSessionId;
		private readonly ConcurrentDictionary<Node, MetricsNodeCollector> nodes = new();
		private readonly ConcurrentDictionary<Node, MetricsNodeCollector> removedNodes =
			new(ReferenceEqualityComparer.Instance);
		private readonly MetricsNodeCollector removedNodeCollector;
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
		internal readonly long SessionId;
		internal readonly DateTimeOffset StartedAt;
		private volatile HistogramState tendDuration;

		internal bool Active =>
			Volatile.Read(ref lifecycleState) == (int)Lifecycle.Active;
		internal bool CanBegin =>
			Volatile.Read(ref lifecycleState) == (int)Lifecycle.Active;
		internal bool AcceptsWrites =>
			Volatile.Read(ref lifecycleState) != (int)Lifecycle.Frozen;
		internal bool OperationalEnabled => CanBegin && config.OperationalEnabled;

		internal MetricsRuntimeConfig RuntimeConfig => config;

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

		internal void ReleaseWrite()
		{
			Interlocked.Decrement(ref activeWriters);
		}

		internal MetricsSession(
			MetricsPolicy policy,
			IMetricsClock clock = null,
			IMetricsRandom random = null)
		{
			SessionId = Interlocked.Increment(ref nextSessionId);
			StartedAt = DateTimeOffset.UtcNow;
			config = new MetricsRuntimeConfig(policy);
			removedNodeCollector = new MetricsNodeCollector(config);
			Clock = clock ?? SystemMetricsClock.Instance;
			this.random = random ?? ThreadMetricsRandom.Instance;
			tendDuration = new HistogramState(config.Shape);
			using Process process = Process.GetCurrentProcess();
			processTimestamp = DateTime.UtcNow;
			processCpu = process.TotalProcessorTime;
		}

		internal CommandMetricsHandle BeginCommand(string ns, LatencyType type, bool countCommand)
		{
			using MetricsBeginPermit permit = AcquireBegin();
			MetricsRuntimeConfig current = config;

			if (!permit.Acquired || !current.OperationalEnabled || !Sample(current))
			{
				return default;
			}

			if (countCommand)
			{
				Interlocked.Increment(ref commandCount);
			}
			return new CommandMetricsHandle(new CommandMetricsState(this, ns, type));
		}

		internal CommandMetricsHandle BeginApiCommand(string ns, LatencyType type)
		{
			using MetricsBeginPermit permit = AcquireBegin();
			return permit.Acquired
				? BeginApiCommandAdmitted(ns, type, config)
				: default;
		}

		internal CommandMetricsHandle BeginApiCommand(
			string ns,
			LatencyType type,
			ApiExecutionMode mode,
			out MetricsRuntimeConfig operationalConfig)
		{
			using MetricsBeginPermit permit = AcquireBegin();

			if (!permit.Acquired)
			{
				operationalConfig = null;
				return default;
			}

			MetricsRuntimeConfig current = config;
			AddUsageAdmitted(mode, current);
			operationalConfig = current.OperationalEnabled ? current : null;
			return BeginApiCommandAdmitted(ns, type, current);
		}

		private CommandMetricsHandle BeginApiCommandAdmitted(
			string ns,
			LatencyType type,
			MetricsRuntimeConfig current)
		{
			if (!current.OperationalEnabled || !Sample(current))
			{
				return default;
			}

			Interlocked.Increment(ref commandCount);
			return new CommandMetricsHandle(new CommandMetricsState(this, ns, type, true));
		}

		internal CommandMetricsHandle BeginSampledCommand(string ns, LatencyType type)
		{
			using MetricsBeginPermit permit = AcquireBegin();
			return permit.Acquired && config.OperationalEnabled
				? new CommandMetricsHandle(new CommandMetricsState(this, ns, type))
				: default;
		}

		internal CommandMetricsHandle BeginCapturedCommand(
			string ns,
			LatencyType type,
			MetricsRuntimeConfig capturedConfig = null)
		{
			using MetricsWritePermit permit = AcquireWrite();
			MetricsRuntimeConfig current = capturedConfig ?? config;
			return permit.Acquired && current.OperationalEnabled && Sample(current)
				? new CommandMetricsHandle(new CommandMetricsState(this, ns, type))
				: default;
		}

		internal CommandMetricsHandle BeginCapturedSampledCommand(
			string ns,
			LatencyType type)
		{
			using MetricsWritePermit permit = AcquireWrite();
			return permit.Acquired
				? new CommandMetricsHandle(new CommandMetricsState(this, ns, type))
				: default;
		}

		internal bool CountCommand()
		{
			using MetricsBeginPermit permit = AcquireBegin();
			MetricsRuntimeConfig current = config;

			if (permit.Acquired && current.OperationalEnabled && Sample(current))
			{
				Interlocked.Increment(ref commandCount);
				return true;
			}
			return false;
		}

		internal CommandCountMetricsHandle BeginApiCoordinator()
		{
			using MetricsBeginPermit permit = AcquireBegin();
			return permit.Acquired
				? BeginApiCoordinatorAdmitted(config)
				: default;
		}

		internal QueryMetricsHandle BeginQueryMetrics()
		{
			using MetricsBeginPermit permit = AcquireBegin();
			return permit.Acquired && config.OperationalEnabled
				? new QueryMetricsHandle(this)
				: default;
		}

		internal CommandCountMetricsHandle BeginApiCoordinator(
			ApiExecutionMode mode,
			out MetricsRuntimeConfig operationalConfig)
		{
			using MetricsBeginPermit permit = AcquireBegin();

			if (!permit.Acquired)
			{
				operationalConfig = null;
				return default;
			}

			MetricsRuntimeConfig current = config;
			AddUsageAdmitted(mode, current);
			operationalConfig = current.OperationalEnabled ? current : null;
			return BeginApiCoordinatorAdmitted(current);
		}

		private CommandCountMetricsHandle BeginApiCoordinatorAdmitted(
			MetricsRuntimeConfig current)
		{
			if (!current.OperationalEnabled)
			{
				return default;
			}

			bool sampled = Sample(current);

			if (sampled)
			{
				Interlocked.Increment(ref commandCount);
			}
			return new CommandCountMetricsHandle(this, sampled);
		}

		internal void AddCommandRetry()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				Interlocked.Increment(ref commandRetries);
			}
		}

		internal void AddTimeout(bool client)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (!permit.Acquired)
			{
				return;
			}
			AddTimeoutAdmitted(client);
		}

		internal void AddTimeoutAdmitted(bool client)
		{
			if (client)
			{
				Interlocked.Increment(ref clientTimeouts);
			}
			else
			{
				Interlocked.Increment(ref serverTimeouts);
			}
		}

		internal void AddBatchPartialFailure()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				Interlocked.Increment(ref batchPartialFailures);
			}
		}

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
				GetNode(node, runtimeConfig).GetNamespace(
					ns,
					runtimeConfig.Shape).AddLatency(
					LatencyType.CONN,
					Clock.GetElapsedNanoseconds(startTimestamp),
					runtimeConfig.Shape);
			}
		}

		internal void AddBytes(Node node, string ns, long bytesIn, long bytesOut)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				AddBytesAdmitted(node, ns, bytesIn, bytesOut);
			}
		}

		internal void AddBytesAdmitted(
			Node node,
			string ns,
			long bytesIn,
			long bytesOut)
		{
			MetricsRuntimeConfig current = config;

			if (current.OperationalEnabled)
			{
				AddCapturedBytesAdmitted(node, ns, bytesIn, bytesOut);
			}
		}

		internal void AddCapturedBytesAdmitted(
			Node node,
			string ns,
			long bytesIn,
			long bytesOut)
		{
			MetricsRuntimeConfig current = config;
			GetNode(node, current).GetNamespace(ns, current.Shape).AddBytes(
				bytesIn,
				bytesOut);
		}

		internal void AddLatency(
			Node node,
			string ns,
			LatencyType type,
			long elapsedNanoseconds)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				AddLatencyAdmitted(node, ns, type, elapsedNanoseconds);
			}
		}

		internal void AddLatencyAdmitted(
			Node node,
			string ns,
			LatencyType type,
			long elapsedNanoseconds)
		{
			MetricsRuntimeConfig current = config;

			if (current.OperationalEnabled)
			{
				AddCapturedLatencyAdmitted(node, ns, type, elapsedNanoseconds);
			}
		}

		internal void AddCapturedLatencyAdmitted(
			Node node,
			string ns,
			LatencyType type,
			long elapsedNanoseconds)
		{
			MetricsRuntimeConfig current = config;
			GetNode(node, current).GetNamespace(ns, current.Shape).AddLatency(
				type,
				elapsedNanoseconds,
				current.Shape);
		}

		internal void AddFailure(Node node, string ns, MetricsFailure failure)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				AddFailureAdmitted(node, ns, failure);
			}
		}

		internal void AddFailureAdmitted(
			Node node,
			string ns,
			MetricsFailure failure)
		{
			MetricsRuntimeConfig current = config;

			if (current.OperationalEnabled)
			{
				AddCapturedFailureAdmitted(node, ns, failure);
			}
		}

		internal void AddCapturedFailureAdmitted(
			Node node,
			string ns,
			MetricsFailure failure)
		{
			MetricsRuntimeConfig current = config;
			GetNode(node, current).GetNamespace(ns, current.Shape).AddFailure(failure);
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

		internal MetricsNodeCollector GetNode(Node node)
		{
			return GetNode(node, config);
		}

		private MetricsNodeCollector GetNode(Node node, MetricsRuntimeConfig expected)
		{
			if (!node.Active)
			{
				removedNodeCollector.EnsureShape(expected.Shape);
				return removedNodeCollector;
			}

			MetricsNodeCollector collector =
				nodes.GetOrAdd(node, _ => new MetricsNodeCollector(expected));
			collector.EnsureShape(expected.Shape);

			if (!node.Active)
			{
				if (nodes.TryRemove(node, out MetricsNodeCollector removed))
				{
					removedNodes.TryAdd(node, removed);
				}
				removedNodeCollector.EnsureShape(expected.Shape);
				return removedNodeCollector;
			}
			return collector;
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

		internal void AddConnectionClosed(Node node, ConnectionCloseReason reason)
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired)
			{
				Interlocked.Increment(ref connectionsClosed);
				MetricsRuntimeConfig current = config;
				GetNode(node, current).AddClosed(
					reason,
					current.OperationalEnabled);
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

		internal void RemoveNode(Node node)
		{
			if (nodes.TryRemove(node, out MetricsNodeCollector removed))
			{
				removedNodes.TryAdd(node, removed);
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

		internal MetricsSessionView Read(MetricsRuntimeConfig expected = null)
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

				foreach (KeyValuePair<Node, MetricsNodeCollector> entry in nodes)
				{
					MetricsNodeView nodeView = entry.Value.Read(entry.Key.Name, current);
					nodeViews[entry.Key.Name] = nodeView;
				}

				MetricsNodeCollector removedAggregate = new(current);
				removedAggregate.Merge(
					removedNodeCollector.Read(string.Empty, current),
					current);

				foreach (MetricsNodeCollector collector in removedNodes.Values)
				{
					removedAggregate.Merge(
						collector.Read(string.Empty, current),
						current);
				}
				MetricsNodeView removedNodeView =
					removedAggregate.Read(string.Empty, current);
				IReadOnlyList<long> tendBuckets =
					Array.AsReadOnly(EnsureTend(current).Histogram.CopyBuckets());

				if (!ReferenceEquals(config, current))
				{
					if (expected != null)
					{
						return null;
					}
					continue;
				}

				return new MetricsSessionView(
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
				removedNodeView);
			}
		}

		private HistogramState EnsureTend(MetricsRuntimeConfig expected)
		{
			HistogramState current = tendDuration;

			if (current.ShapeGeneration >= expected.Shape.Generation)
			{
				return current;
			}

			lock (tendLock)
			{
				current = tendDuration;

				if (current.ShapeGeneration < expected.Shape.Generation)
				{
					current = new HistogramState(expected.Shape);
					tendDuration = current;
				}
				return current;
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

	internal sealed record MetricsNamespaceView(
		string Namespace,
		long Errors,
		long Timeouts,
		long KeyBusy,
		long RecordTooBig,
		long DeviceOverload,
		long BytesIn,
		long BytesOut,
		IReadOnlyDictionary<LatencyType, IReadOnlyList<long>> Latency);

	internal sealed record MetricsNodeView(
		string Name,
		long Opened,
		long Closed,
		long PoolExhausted,
		long ClosedError,
		long ClosedIdle,
		long ClosedPoolShrink,
		long ClosedNodeRemoved,
		IReadOnlyDictionary<string, MetricsNamespaceView> Namespaces);

	internal sealed record MetricsSessionView(
		long SessionId,
		DateTimeOffset StartedAt,
		long ConfigGeneration,
		long ShapeGeneration,
		bool Active,
		bool OperationalEnabled,
		bool UsageEnabled,
		MetricsLatencyUnit LatencyUnit,
		int LatencyColumns,
		int LatencyShift,
		ulong SampleRange,
		ulong SampleThreshold,
		long CommandCount,
		long CommandRetries,
		long ClientTimeouts,
		long ServerTimeouts,
		long BatchPartialFailures,
		long InvalidNodes,
		long ConnectionOpenFailures,
		long ConnectionTlsFailures,
		long ConnectionAuthFailures,
		long ConnectionsOpened,
		long ConnectionsClosed,
		long RecoverEnqueued,
		long RecoverCompleted,
		long RecoverAborted,
		long TimeoutImmediate,
		long QueryRecordsQueued,
		long QueryServersConcurrent,
		long QueryAbandoned,
		IReadOnlyList<long> TendDuration,
		long ApiBlocking,
		long ApiDeferred,
		long ApiBackground,
		IReadOnlyDictionary<string, MetricsNodeView> Nodes,
		MetricsNodeView RemovedNodes)
	{
		internal MetricsRuntimeView Runtime { get; init; }
	}

	internal sealed record MetricsRuntimeNodeView(
		string Name,
		string Address,
		int Port,
		int ConnectionsInPool,
		int ConnectionsInUse,
		int ErrorRate);

	internal sealed record MetricsRuntimeView(
		DateTime Timestamp,
		string ClientType,
		string ClientVersion,
		string ClusterName,
		string AppId,
		IReadOnlyDictionary<string, string> Labels,
		double CpuPercent,
		long MemoryBytes,
		int ActiveNodes,
		int InvalidNodes,
		int RecoverQueueSize,
		int ConnectionsInPool,
		int ConnectionsInUse,
		IReadOnlyList<MetricsRuntimeNodeView> Nodes);

	internal enum ConnectionFailurePhase
	{
		Open,
		Tls,
		Auth
	}
}