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

	internal sealed class MetricsStore
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

		internal MetricsNodeGeneration RegisterNode(Node node)
		{
			MetricsRuntimeConfig current = config;
			MetricsNodeGeneration generation = nodes.GetOrAdd(
				node,
				key =>
				{
					MetricsNodeGeneration created = new(
						this,
						key.Name,
						current);
					nodeNames.GetOrAdd(
						created.Name,
						name => new MetricsNodeNameState()).Add(created);
					return created;
				});
			generation.Collector.EnsureShape(current.Shape);
			return generation;
		}

		internal void AdmitNode(Node node)
		{
			MetricsNodeGeneration generation = RegisterNode(node);
			nodeNames[generation.Name].Admit(generation);
		}

		internal void InvalidateNode(Node node)
		{
			if (nodes.TryGetValue(node, out MetricsNodeGeneration generation))
			{
				generation.InvalidatePublicReads();
			}
		}

		internal void AddCompatibilityError(
			MetricsNodeGeneration generation,
			string ns)
		{
			if (generation.PublicReadsAllowed)
			{
				generation.AddError(ns);
			}
		}

		internal void AddCompatibilityTimeout(
			MetricsNodeGeneration generation,
			string ns)
		{
			if (generation.PublicReadsAllowed)
			{
				generation.AddTimeout(ns);
			}
		}

		internal void AddCompatibilityKeyBusy(
			MetricsNodeGeneration generation,
			string ns)
		{
			if (generation.PublicReadsAllowed)
			{
				generation.AddKeyBusy(ns);
			}
		}

		internal long GetCompatibilityError(
			MetricsNodeGeneration generation,
			string ns = null)
		{
			return generation.GetErrorCount(ns);
		}

		internal long GetCompatibilityTimeout(
			MetricsNodeGeneration generation,
			string ns = null)
		{
			return generation.GetTimeoutCount(ns);
		}

		internal long GetCompatibilityKeyBusy(
			MetricsNodeGeneration generation,
			string ns = null)
		{
			return generation.GetKeyBusyCount(ns);
		}

		internal long GetCompatibilityError(Node node)
		{
			return RegisterNode(node).GetErrorCount();
		}

		internal MetricsNodeView ReadCompatibilityNode(Node node)
		{
			MetricsNodeGeneration generation = RegisterNode(node);
			return nodeNames[generation.Name].ReadCompatibility(
				generation.Name,
				config);
		}

		internal MetricsNodeView ReadCompatibilityGeneration(Node node)
		{
			return RegisterNode(node).ReadCompatibility(config);
		}

		internal void RecordCompatibilityFailure(
			Node node,
			string ns,
			AerospikeException exception,
			bool clientTimeout)
		{
			if (node == null)
			{
				return;
			}

			MetricsNodeGeneration generation = RegisterNode(node);

			if (exception?.Result == ResultCode.TIMEOUT || clientTimeout)
			{
				generation.AddTimeout(ns);
			}
			else if (exception?.Result == ResultCode.KEY_BUSY)
			{
				generation.AddError(ns);
				generation.AddKeyBusy(ns);
			}
			else
			{
				generation.AddError(ns);
			}
		}

		internal NodeMetrics CreateNodeMetrics(Node node, MetricsPolicy policy)
		{
			RegisterNode(node);
			return new NodeMetrics(policy);
		}

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

		internal CommandMetricsHandle BeginCommand(
			string ns,
			IReadOnlyList<string> namespaces,
			LatencyType type,
			bool countCommand)
		{
			using MetricsBeginPermit permit = AcquireBegin();
			MetricsRuntimeConfig current = config;

			if (!permit.Acquired || !current.OperationalEnabled)
			{
				return default;
			}

			bool sampled = Sample(current);

			if (sampled && countCommand)
			{
				Interlocked.Increment(ref commandCount);
			}
			return new CommandMetricsHandle(
				new CommandMetricsState(
					this,
					ns,
					namespaces,
					type,
					sampled,
					MetricsAttemptMode.Operational));
		}

		internal CommandMetricsHandle BeginCommand(
			string ns,
			LatencyType type,
			bool countCommand)
		{
			return BeginCommand(ns, null, type, countCommand);
		}

		internal bool BeginApiCall(
			ApiMetricsOperation operation,
			ApiExecutionMode mode,
			out bool sampled)
		{
			sampled = false;

			if (operation == ApiMetricsOperation.Uninstrumented)
			{
				return false;
			}

			using MetricsBeginPermit permit = AcquireBegin();

			if (!permit.Acquired)
			{
				return false;
			}

			MetricsRuntimeConfig current = config;
			AddUsageAdmitted(mode, current);

			if (!current.OperationalEnabled)
			{
				return false;
			}

			sampled = Sample(current);

			if (sampled)
			{
				Interlocked.Increment(ref commandCount);
			}
			return true;
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
			return new CommandMetricsHandle(
				new CommandMetricsState(
					this,
					ns,
					null,
					type,
					true,
					MetricsAttemptMode.Operational,
					true));
		}

		internal CommandMetricsHandle BeginSampledCommand(string ns, LatencyType type)
		{
			using MetricsBeginPermit permit = AcquireBegin();
			return permit.Acquired && config.OperationalEnabled
				? new CommandMetricsHandle(
					new CommandMetricsState(
						this,
						ns,
						null,
						type,
						true,
						MetricsAttemptMode.Operational))
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

		internal CommandCountMetricsHandle BeginCountOnly()
		{
			using MetricsBeginPermit permit = AcquireBegin();

			if (!permit.Acquired)
			{
				return default;
			}

			MetricsRuntimeConfig current = config;

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

		internal void AddDelayQueueTimeout()
		{
			using MetricsWritePermit permit = AcquireWrite();

			if (permit.Acquired && config.OperationalEnabled)
			{
				Interlocked.Increment(ref delayQueueTimeouts);
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

		internal void RecordAttemptSuccess(
			Node node,
			IReadOnlyList<string> namespaces,
			string bytesNamespace,
			LatencyType type,
			long elapsedNanoseconds,
			long bytesIn,
			long bytesOut,
			bool sampled = true,
			long sessionId = 0)
		{
			if (!sampled)
			{
				return;
			}

			using MetricsWritePermit permit = sessionId == 0
				? AcquireWrite()
				: AcquireWrite(sessionId);

			if (!permit.Acquired)
			{
				return;
			}

			MetricsRuntimeConfig current = config;
			MetricsNodeGeneration generation = RegisterNode(node);
			if (node.Active && node.MetricsWasAdmitted)
			{
				nodeNames[generation.Name].Admit(generation);
			}
			using NodeMetricsLease lease = node.AcquireMetrics();
			NodeMetrics metrics = lease.Metrics;

			if (metrics != null && generation.OwnsMetrics(metrics))
			{
				metrics.RecordSuccess(
					namespaces,
					bytesNamespace,
					type,
					elapsedNanoseconds,
					bytesIn,
					bytesOut);
			}
			else
			{
				if (bytesNamespace != null)
				{
					generation.Collector.GetNamespace(
						bytesNamespace,
						current.Shape).AddBytes(bytesIn, bytesOut);
				}
				foreach (string ns in namespaces)
				{
					generation.Collector.GetNamespace(
						ns,
						current.Shape).AddLatency(
							type,
							elapsedNanoseconds,
							current.Shape);
				}
			}
		}

		internal void RecordCallSuccess(
			ApiMetricsOperation operation,
			long startTimestamp,
			long sessionId = 0)
		{
			LatencyType type = operation switch
			{
				ApiMetricsOperation.Read => LatencyType.READ,
				ApiMetricsOperation.Write => LatencyType.WRITE,
				ApiMetricsOperation.Batch => LatencyType.BATCH,
				ApiMetricsOperation.Query => LatencyType.QUERY,
				_ => LatencyType.NONE
			};

			if (type == LatencyType.NONE)
			{
				return;
			}

			using MetricsWritePermit permit = sessionId == 0
				? AcquireWrite()
				: AcquireWrite(sessionId);

			if (permit.Acquired)
			{
				MetricsRuntimeConfig current = config;
				callCollector.AddLatency(
					type,
					Clock.GetElapsedNanoseconds(startTimestamp),
					current.Shape);
			}
		}

		internal void RecordAttemptFailure(
			Node node,
			IReadOnlyList<string> namespaces,
			MetricsFailure failure,
			bool clientTimeout,
			long sessionId = 0)
		{
			using MetricsWritePermit permit = sessionId == 0
				? AcquireWrite()
				: AcquireWrite(sessionId);

			if (!permit.Acquired)
			{
				return;
			}

			if (failure == MetricsFailure.Timeout)
			{
				AddTimeoutAdmitted(clientTimeout);
				MetricsRuntimeConfig current = config;
				MetricsNodeCollector collector = GetNode(node, current);

				foreach (string ns in namespaces)
				{
					collector.GetNamespace(ns, current.Shape)
						.AddTimeoutSplit(clientTimeout);
				}
			}

			foreach (string ns in namespaces)
			{
				AddCapturedFailureAdmitted(node, ns, failure);
			}
		}

		internal void RecordBatchRecordFailure(
			Node node,
			string ns,
			MetricsFailure failure,
			long sessionId)
		{
			using MetricsWritePermit permit = AcquireWrite(sessionId);

			if (!permit.Acquired)
			{
				return;
			}

			if (failure == MetricsFailure.Timeout)
			{
				AddTimeoutAdmitted(false);
				MetricsRuntimeConfig current = config;
				GetNode(node, current)
					.GetNamespace(ns, current.Shape)
					.AddTimeoutSplit(false);
			}
			AddCapturedFailureAdmitted(node, ns, failure);
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

		internal MetricsNodeCollector GetNode(Node node)
		{
			return GetNode(node, config);
		}

		private MetricsNodeCollector GetNode(Node node, MetricsRuntimeConfig expected)
		{
			MetricsNodeGeneration generation = RegisterNode(node);

			if (node.Active && node.MetricsWasAdmitted)
			{
				nodeNames[generation.Name].Admit(generation);
			}
			generation.Collector.EnsureShape(expected.Shape);
			return generation.Collector;
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

		internal void RemoveNode(Node node)
		{
			BeginNodeDeparture(node);
		}

		internal MetricsNodeDeparture BeginNodeDeparture(Node node)
		{
			return BeginNodeDeparture(node, null);
		}

		internal MetricsNodeDeparture BeginNodeDeparture(
			Node node,
			Action collectorResolved)
		{
			MetricsNodeDeparture current = node.MetricsDeparture;
			if (current != null &&
				ReferenceEquals(current.Store, this))
			{
				return current;
			}

			MetricsNodeGeneration generation = RegisterNode(node);
			collectorResolved?.Invoke();
			current = new MetricsNodeDeparture(this, generation.Collector);
			node.InstallMetricsDeparture(current);
			nodeNames[generation.Name].Depart(generation);
			return current;
		}

		internal void RecordNodeDeparture(
			Node node,
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			MetricsNodeGeneration generation = RegisterNode(node);
			RecordNodeDeparture(
				generation.Name,
				node.address.Address.ToString(),
				node.address.Port,
				node.GetErrorRate(),
				nodeNames[generation.Name],
				compatibility);
		}

		internal void RecordNodeDeparture(
			string name,
			string address,
			int port,
			int errorRate,
			MetricsNodeNameState state,
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			lock (departureLock)
			{
				if (!departures.TryGetValue(
					name,
					out DepartureEntry entry))
				{
					if (departures.Count >= MaximumDepartureNames)
					{
						DepartureEntry reusable = departures.Values
							.Where(candidate =>
								candidate.Acknowledged != null &&
								!candidate.State.IsActive &&
								MetricsNodeNameState.IsZero(
									Subtract(
										candidate.State.Read(
											candidate.Name,
											config),
										candidate.Acknowledged)))
							.OrderBy(candidate => candidate.Generation)
							.FirstOrDefault();

						if (reusable == null)
						{
							departureTruncationGeneration++;
							return;
						}
						departures.Remove(reusable.Name);
					}

					entry = new DepartureEntry(name, state);
					departures.Add(name, entry);
				}

				entry.Address = address ?? string.Empty;
				entry.Port = port;
				entry.ErrorRate = errorRate;
				entry.Generation = ++departureGeneration;
				entry.Acknowledged = null;
				entry.Compatibility = MergeCompatibilityNodes(
					entry.Compatibility,
					compatibility,
					config.Shape);
			}
		}

		internal MetricsDepartureCapture CaptureDepartures(
			MetricsRuntimeConfig expected)
		{
			return CaptureDepartures(expected, null);
		}

		private MetricsDepartureCapture CaptureDepartures(
			MetricsRuntimeConfig expected,
			IReadOnlyDictionary<string, MetricsNodeCapture> captures)
		{
			if (expected == null)
			{
				return MetricsDepartureCapture.Empty;
			}

			lock (departureLock)
			{
				List<MetricsDepartedNodeCapture> nodes =
					new(departures.Count);
				List<MetricsDepartureWatermark> watermarks = new();

				foreach (DepartureEntry entry in departures.Values.OrderBy(
					entry => entry.Name,
					StringComparer.Ordinal))
				{
					MetricsNodeCapture capture =
						captures != null &&
						captures.TryGetValue(
							entry.Name,
							out MetricsNodeCapture saved)
							? saved
							: entry.State.Capture(entry.Name, expected);
					MetricsNodeView current = capture.Public;
					MetricsNodeView delta = entry.Acknowledged == null
						? current
						: Subtract(current, entry.Acknowledged);

					if (!capture.Active &&
						(entry.Acknowledged == null ||
							!MetricsNodeNameState.IsZero(delta)))
					{
						nodes.Add(new MetricsDepartedNodeCapture(
							entry.Name,
							entry.Address,
							entry.Port,
							entry.ErrorRate,
							delta,
							BuildCompatibilitySnapshot(
								entry,
								capture.Compatibility,
								expected.Shape)));
					}

					watermarks.Add(new MetricsDepartureWatermark(
						entry.Name,
						entry.Generation,
						current,
						expected.Shape));
				}

				MetricsDepartureToken token = new(
					this,
					watermarks,
					departureTruncationGeneration);
				return new MetricsDepartureCapture(
					nodes,
					departureTruncationGeneration != 0,
					token);
			}
		}

		internal void AcknowledgeDepartures(MetricsDepartureToken token)
		{
			if (token == null || !ReferenceEquals(token.Store, this))
			{
				return;
			}

			lock (departureLock)
			{
				foreach (MetricsDepartureWatermark watermark in token.Watermarks)
				{
					if (!departures.TryGetValue(
						watermark.Name,
						out DepartureEntry entry) ||
						entry.Generation != watermark.Generation)
					{
						continue;
					}

					entry.Acknowledged = watermark.Captured;
				}

				if (token.TruncationGeneration != 0 &&
					token.TruncationGeneration ==
						departureTruncationGeneration)
				{
					departureTruncationGeneration = 0;
				}
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

		private static MetricsNodeView Subtract(
			MetricsNodeView current,
			MetricsNodeView baseline)
		{
			Dictionary<string, MetricsNamespaceView> namespaces =
				new(StringComparer.Ordinal);
			HashSet<string> names = new(
				current.Namespaces.Keys,
				StringComparer.Ordinal);
			names.UnionWith(baseline.Namespaces.Keys);

			foreach (string name in names)
			{
				current.Namespaces.TryGetValue(
					name,
					out MetricsNamespaceView currentNamespace);
				baseline.Namespaces.TryGetValue(
					name,
					out MetricsNamespaceView baselineNamespace);
				namespaces[name] = SubtractNamespace(
					name,
					currentNamespace,
					baselineNamespace);
			}

			return new MetricsNodeView(
				current.Name,
				Subtract(current.Opened, baseline.Opened),
				Subtract(current.Closed, baseline.Closed),
				Subtract(current.PoolExhausted, baseline.PoolExhausted),
				Subtract(current.OpenFailures, baseline.OpenFailures),
				Subtract(
					current.TlsHandshakeFailures,
					baseline.TlsHandshakeFailures),
				Subtract(current.AuthFailures, baseline.AuthFailures),
				SubtractBuckets(
					current.ConnectionLatency,
					baseline.ConnectionLatency),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(
					namespaces));
		}

		private static MetricsNamespaceView SubtractNamespace(
			string name,
			MetricsNamespaceView current,
			MetricsNamespaceView baseline)
		{
			Dictionary<LatencyType, IReadOnlyList<long>> latency = new();

			for (int i = 0; i < Latency.GetMax(); i++)
			{
				LatencyType type = (LatencyType)i;
				IReadOnlyList<long> currentBuckets =
					GetBuckets(current, type);
				IReadOnlyList<long> baselineBuckets =
					GetBuckets(baseline, type);
				int count = Math.Max(
					currentBuckets.Count,
					baselineBuckets.Count);
				long[] delta = new long[count];

				for (int j = 0; j < count; j++)
				{
					long currentValue =
						j < currentBuckets.Count ? currentBuckets[j] : 0;
					long baselineValue =
						j < baselineBuckets.Count ? baselineBuckets[j] : 0;
					delta[j] = Subtract(currentValue, baselineValue);
				}
				latency[type] = Array.AsReadOnly(delta);
			}

			return new MetricsNamespaceView(
				name,
				Subtract(current?.Errors ?? 0, baseline?.Errors ?? 0),
				Subtract(current?.Timeouts ?? 0, baseline?.Timeouts ?? 0),
				Subtract(
					current?.ClientTimeouts ?? 0,
					baseline?.ClientTimeouts ?? 0),
				Subtract(
					current?.ServerTimeouts ?? 0,
					baseline?.ServerTimeouts ?? 0),
				Subtract(current?.KeyBusy ?? 0, baseline?.KeyBusy ?? 0),
				Subtract(
					current?.RecordTooBig ?? 0,
					baseline?.RecordTooBig ?? 0),
				Subtract(
					current?.DeviceOverload ?? 0,
					baseline?.DeviceOverload ?? 0),
				Subtract(current?.BytesIn ?? 0, baseline?.BytesIn ?? 0),
				Subtract(current?.BytesOut ?? 0, baseline?.BytesOut ?? 0),
				new ReadOnlyDictionary<LatencyType, IReadOnlyList<long>>(
					latency));
		}

		private static IReadOnlyList<long> GetBuckets(
			MetricsNamespaceView view,
			LatencyType type)
		{
			return view != null &&
				view.Latency.TryGetValue(
					type,
					out IReadOnlyList<long> buckets)
					? buckets
					: Array.Empty<long>();
		}

		private static long Subtract(long value, long baseline)
		{
			return value >= baseline ? value - baseline : value;
		}

		private static IReadOnlyList<long> SubtractBuckets(
			IReadOnlyList<long> value,
			IReadOnlyList<long> baseline)
		{
			int count = Math.Max(value.Count, baseline.Count);
			long[] result = new long[count];

			for (int i = 0; i < count; i++)
			{
				long current = i < value.Count ? value[i] : 0;
				long previous = i < baseline.Count ? baseline[i] : 0;
				result[i] = Subtract(current, previous);
			}
			return Array.AsReadOnly(result);
		}

		private static MetricsNodeCompatibilitySnapshot
			BuildCompatibilitySnapshot(
				DepartureEntry entry,
				MetricsNodeView view,
				MetricsHistogramShape shape)
		{
			MetricsNodeCompatibilitySnapshot template =
				NormalizeCompatibility(entry.Compatibility, shape);

			if (template == null)
			{
				return null;
			}

			int asyncOpened = template.AsyncConnections.opened;
			int asyncClosed = template.AsyncConnections.closed;
			ConnectionStats sync = new(
				template.SyncConnections.inPool,
				template.SyncConnections.inUse,
				(int)Math.Min(
					int.MaxValue,
					Math.Max(0, view.Opened - asyncOpened)),
				(int)Math.Min(
					int.MaxValue,
					Math.Max(0, view.Closed - asyncClosed)));
			List<MetricsNamespaceCompatibilitySnapshot> namespaces =
				new();
			Dictionary<string, MetricsNamespaceCompatibilitySnapshot>
				templateNamespaces = template.Namespaces.ToDictionary(
					ns => ns.Name,
					StringComparer.Ordinal);
			HashSet<string> names = new(
				view.Namespaces.Keys,
				StringComparer.Ordinal);
			names.UnionWith(templateNamespaces.Keys);

			foreach (string name in names.OrderBy(
				name => name,
				StringComparer.Ordinal))
			{
				view.Namespaces.TryGetValue(
					name,
					out MetricsNamespaceView ns);
				templateNamespaces.TryGetValue(
					name,
					out MetricsNamespaceCompatibilitySnapshot saved);
				namespaces.Add(new MetricsNamespaceCompatibilitySnapshot(
					name,
					Math.Max(ns?.Errors ?? 0, saved?.Errors ?? 0),
					Math.Max(ns?.Timeouts ?? 0, saved?.Timeouts ?? 0),
					Math.Max(ns?.KeyBusy ?? 0, saved?.KeyBusy ?? 0),
					Math.Max(ns?.BytesIn ?? 0, saved?.BytesIn ?? 0),
					Math.Max(ns?.BytesOut ?? 0, saved?.BytesOut ?? 0),
					Enumerable.Range(0, Latency.GetMax()).Select(index =>
					{
						IReadOnlyList<long> current =
							ns != null &&
							ns.Latency.TryGetValue(
								(LatencyType)index,
								out IReadOnlyList<long> buckets)
								? buckets
								: Array.Empty<long>();
						IReadOnlyList<long> previous =
							saved != null && index < saved.Latency.Count
								? saved.Latency[index]
								: Array.Empty<long>();
						return (IReadOnlyList<long>)Array.AsReadOnly(
							Enumerable.Range(0, shape.Columns)
								.Select(bucket => Math.Max(
									bucket < current.Count
										? current[bucket]
										: 0,
									bucket < previous.Count
										? previous[bucket]
										: 0))
								.ToArray());
					})));
			}

			return new MetricsNodeCompatibilitySnapshot(
				entry.Name,
				entry.Address,
				entry.Port,
				sync,
				template.AsyncConnections,
				namespaces,
				shape.Unit,
				shape.Columns,
				shape.Shift);
		}

		private static MetricsNodeCompatibilitySnapshot
			MergeCompatibilityNodes(
				MetricsNodeCompatibilitySnapshot previous,
				MetricsNodeCompatibilitySnapshot current,
				MetricsHistogramShape shape)
		{
			if (previous == null)
			{
				return NormalizeCompatibility(current, shape);
			}
			if (current == null)
			{
				return NormalizeCompatibility(previous, shape);
			}

			previous = NormalizeCompatibility(previous, shape);
			current = NormalizeCompatibility(current, shape);
			Dictionary<string, MetricsNamespaceCompatibilitySnapshot> left =
				previous.Namespaces.ToDictionary(
					ns => ns.Name,
					StringComparer.Ordinal);
			Dictionary<string, MetricsNamespaceCompatibilitySnapshot> right =
				current.Namespaces.ToDictionary(
					ns => ns.Name,
					StringComparer.Ordinal);
			HashSet<string> names = new(left.Keys, StringComparer.Ordinal);
			names.UnionWith(right.Keys);

			IEnumerable<MetricsNamespaceCompatibilitySnapshot> namespaces =
				names.OrderBy(name => name, StringComparer.Ordinal).Select(name =>
				{
					left.TryGetValue(
						name,
						out MetricsNamespaceCompatibilitySnapshot first);
					right.TryGetValue(
						name,
						out MetricsNamespaceCompatibilitySnapshot second);
					return new MetricsNamespaceCompatibilitySnapshot(
						name,
						(first?.Errors ?? 0) + (second?.Errors ?? 0),
						(first?.Timeouts ?? 0) + (second?.Timeouts ?? 0),
						(first?.KeyBusy ?? 0) + (second?.KeyBusy ?? 0),
						(first?.BytesIn ?? 0) + (second?.BytesIn ?? 0),
						(first?.BytesOut ?? 0) + (second?.BytesOut ?? 0),
						Enumerable.Range(0, Latency.GetMax()).Select(index =>
							(IReadOnlyList<long>)Array.AsReadOnly(
								Enumerable.Range(0, shape.Columns)
									.Select(bucket =>
										(first != null
											? first.Latency[index][bucket]
											: 0) +
										(second != null
											? second.Latency[index][bucket]
											: 0))
									.ToArray())));
				});

			return new MetricsNodeCompatibilitySnapshot(
				current.Name,
				current.Address,
				current.Port,
				new ConnectionStats(
					current.SyncConnections.inPool,
					current.SyncConnections.inUse,
					AddInt(
						previous.SyncConnections.opened,
						current.SyncConnections.opened),
					AddInt(
						previous.SyncConnections.closed,
						current.SyncConnections.closed)),
				new ConnectionStats(
					current.AsyncConnections.inPool,
					current.AsyncConnections.inUse,
					AddInt(
						previous.AsyncConnections.opened,
						current.AsyncConnections.opened),
					AddInt(
						previous.AsyncConnections.closed,
						current.AsyncConnections.closed)),
				namespaces,
				shape.Unit,
				shape.Columns,
				shape.Shift);
		}

		private static int AddInt(int left, int right)
		{
			return (int)Math.Min(int.MaxValue, (long)left + right);
		}

		private static MetricsNodeCompatibilitySnapshot NormalizeCompatibility(
			MetricsNodeCompatibilitySnapshot node,
			MetricsHistogramShape shape)
		{
			if (node == null ||
				(node.LatencyUnit == shape.Unit &&
					node.LatencyColumns == shape.Columns &&
					node.LatencyShift == shape.Shift))
			{
				return node;
			}

			return new MetricsNodeCompatibilitySnapshot(
				node.Name,
				node.Address,
				node.Port,
				node.SyncConnections,
				node.AsyncConnections,
				node.Namespaces.Select(ns =>
					new MetricsNamespaceCompatibilitySnapshot(
						ns.Name,
						ns.Errors,
						ns.Timeouts,
						ns.KeyBusy,
						ns.BytesIn,
						ns.BytesOut,
						Enumerable.Range(
							0,
							Latency.GetMax()).Select(_ =>
								(IReadOnlyList<long>)Array.AsReadOnly(
									new long[shape.Columns])))),
				shape.Unit,
				shape.Columns,
				shape.Shift);
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
