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
	}
}
