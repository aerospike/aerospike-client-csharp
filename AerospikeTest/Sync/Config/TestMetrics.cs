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
using Aerospike.Client;
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestMetrics : TestSync
	{
		[TestMethod]
		public void MetricsSamplerProbabilityConversion()
		{
			MetricsSampler zero = MetricsSampler.FromProbability(0);
			MetricsSampler half = MetricsSampler.FromProbability(0.5);
			MetricsSampler minimum = MetricsSampler.FromProbability(0.0000005);
			MetricsSampler one = MetricsSampler.FromProbability(1);

			Assert.AreEqual((ulong)1_000_000, zero.Range);
			Assert.AreEqual((ulong)0, zero.Threshold);
			Assert.AreEqual((ulong)500_000, half.Threshold);
			Assert.AreEqual((ulong)1, minimum.Threshold);
			Assert.AreEqual(one.Range, one.Threshold);
		}

		[TestMethod]
		public void MetricsSamplerRejectsInvalidProbability()
		{
			AssertThrows<AerospikeException>(() => MetricsSampler.FromProbability(-0.1));
			AssertThrows<AerospikeException>(() => MetricsSampler.FromProbability(1.1));
			AssertThrows<AerospikeException>(() => MetricsSampler.FromProbability(double.NaN));
		}

		[TestMethod]
		public void MetricsSamplerDrawsOnceAtCommandEntry()
		{
			TestMetricsClock clock = new();
			TestMetricsRandom random = new(4, 5);
			MetricsPolicy policy = CreateInternalPolicy();
			policy.Sampler = new MetricsSampler { Range = 10, Threshold = 5 };
			MetricsSession session = new(policy, clock, random);

			CommandMetricsHandle sampled = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			CommandMetricsHandle rejected = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);

			Assert.IsTrue(sampled.Enabled);
			Assert.IsFalse(rejected.Enabled);
			Assert.AreEqual(2, random.CallCount);
			Assert.AreEqual(1, clock.TimestampCallCount);
			Assert.AreEqual(1L, session.Read().CommandCount);
			Assert.AreEqual(0, session.Read().Nodes.Count);
		}

		[TestMethod]
		public void MetricsDisabledSamplerAvoidsRandomAndClock()
		{
			TestMetricsClock clock = new();
			TestMetricsRandom random = new();
			MetricsPolicy policy = CreateInternalPolicy();
			policy.Sampler = new MetricsSampler { Range = 1, Threshold = 0 };
			MetricsSession session = new(policy, clock, random);

			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);

			Assert.IsFalse(handle.Enabled);
			Assert.AreEqual(0, random.CallCount);
			Assert.AreEqual(0, clock.TimestampCallCount);
			Assert.AreEqual(0L, session.Read().CommandCount);
		}

		[TestMethod]
		public void MetricsDisabledApiOwnershipDoesNotAllocate()
		{
			client.DisableMetrics();
			Cluster cluster = client.Cluster;

			for (int i = 0; i < 100; i++)
			{
				using ApiMetricsCall warmup = cluster.BeginPointApiMetrics(
					SuiteHelpers.ns,
					Latency.LatencyType.READ,
					ApiExecutionMode.Blocking);
			}

			long before = GC.GetAllocatedBytesForCurrentThread();

			for (int i = 0; i < 1_000; i++)
			{
				using ApiMetricsCall metrics = cluster.BeginPointApiMetrics(
					SuiteHelpers.ns,
					Latency.LatencyType.READ,
					ApiExecutionMode.Blocking);
			}
			long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

			Assert.AreEqual(0L, allocated);
		}

		[TestMethod]
		public void MetricsSampledCompositeChildrenDoNotReroll()
		{
			TestMetricsClock clock = new();
			TestMetricsRandom random = new(0, 9);
			MetricsPolicy policy = CreateInternalPolicy();
			policy.Sampler = new MetricsSampler { Range = 10, Threshold = 5 };
			MetricsSession session = new(policy, clock, random);

			Assert.IsTrue(session.CountCommand());
			Assert.IsTrue(session.BeginSampledCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ).Enabled);
			Assert.IsTrue(session.BeginSampledCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.BATCH).Enabled);

			Assert.AreEqual(1, random.CallCount);
			Assert.AreEqual(2, clock.TimestampCallCount);
			Assert.AreEqual(1L, session.Read().CommandCount);
		}

		[TestMethod]
		public void MetricsCommandCommitsSuccessOnceAndReshapesHistograms()
		{
			Node node = client.Cluster.Nodes[0];
			TestMetricsClock clock = new() { ElapsedNanoseconds = 1_999 };
			MetricsPolicy policy = CreateInternalPolicy();
			policy.LatencyUnit = MetricsLatencyUnit.Microseconds;
			policy.LatencyColumns = 4;
			MetricsSession session = new(policy, clock, new TestMetricsRandom(0));
			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);

			handle.AddBytes(node, 12, 34);
			handle.CompleteSuccess(node);
			handle.CompleteSuccess(node);

			MetricsNamespaceView before = GetNamespace(session.Read(), node);
			Assert.AreEqual(12L, before.BytesIn);
			Assert.AreEqual(34L, before.BytesOut);
			CollectionAssert.AreEqual(
				new long[] { 1, 0, 0, 0 },
				before.Latency[Latency.LatencyType.READ].ToArray());

			MetricsPolicy reshaped = new(policy) { LatencyColumns = 5 };
			session.ApplyPolicy(reshaped);

			MetricsNamespaceView after = GetNamespace(session.Read(), node);
			Assert.AreEqual(12L, after.BytesIn);
			Assert.AreEqual(34L, after.BytesOut);
			CollectionAssert.AreEqual(
				new long[] { 0, 0, 0, 0, 0 },
				after.Latency[Latency.LatencyType.READ].ToArray());
		}

		[TestMethod]
		public void MetricsLegacyReshapeKeepsInFlightHistogramUsable()
		{
			MetricsPolicy policy = CreateInternalPolicy();
			using NodeMetrics metrics = new(policy);
			Histograms inFlight = metrics.Histograms;

			metrics.Reshape(new MetricsPolicy(policy) { LatencyColumns = 5 });
			inFlight.AddLatency(SuiteHelpers.ns, Latency.LatencyType.READ, 1);

			Assert.AreNotSame(inFlight, metrics.Histograms);
			Assert.AreEqual(1L, inFlight.GetBuckets(SuiteHelpers.ns)[
				(int)Latency.LatencyType.READ].GetBucket(0));
		}

		[TestMethod]
		public void MetricsNonShapePolicyUpdatePreservesHistograms()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			MetricsSession session = new(
				policy,
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			handle.CompleteSuccess(node);
			MetricsSessionView before = session.Read();

			MetricsPolicy replacement = new(policy)
			{
				UsageEnabled = !policy.UsageEnabled,
				labels = new Dictionary<string, string> { ["region"] = "test" }
			};
			session.ApplyPolicy(replacement);
			MetricsSessionView after = session.Read();

			Assert.AreEqual(before.ConfigGeneration + 1, after.ConfigGeneration);
			Assert.AreEqual(before.ShapeGeneration, after.ShapeGeneration);
			CollectionAssert.AreEqual(
				GetNamespace(before, node).Latency[Latency.LatencyType.READ].ToArray(),
				GetNamespace(after, node).Latency[Latency.LatencyType.READ].ToArray());
		}

		[TestMethod]
		public void MetricsNamespaceNeverReshapesBackward()
		{
			MetricsPolicy firstPolicy = CreateInternalPolicy();
			firstPolicy.LatencyColumns = 4;
			MetricsRuntimeConfig first = new(firstPolicy);
			MetricsNodeCollector collector = new(first);
			MetricsPolicy secondPolicy = new(firstPolicy) { LatencyColumns = 5 };
			MetricsRuntimeConfig second =
				new(secondPolicy, 2, first.Shape);

			collector.EnsureShape(second.Shape);
			MetricsNamespaceCollector ns =
				collector.GetNamespace(SuiteHelpers.ns, first.Shape);
			ns.AddLatency(
				Latency.LatencyType.READ,
				1_000_000,
				first.Shape);

			MetricsNodeView view = collector.Read("node", second);
			Assert.AreEqual(
				5,
				view.Namespaces[SuiteHelpers.ns]
					.Latency[Latency.LatencyType.READ].Count);
			Assert.AreEqual(
				1L,
				view.Namespaces[SuiteHelpers.ns]
					.Latency[Latency.LatencyType.READ].Sum());
		}

		[TestMethod]
		public void MetricsConnectionFailurePhasesAreRecordedOnce()
		{
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			ConnectionMetricsHandle handle =
				new(session, session.StartConnection());

			handle.FailPhase(ConnectionFailurePhase.Auth);
			handle.FailPhase(ConnectionFailurePhase.Auth);
			handle.Fail(ConnectionFailurePhase.Auth);
			handle.Fail(ConnectionFailurePhase.Tls);

			MetricsSessionView view = session.Read();
			Assert.AreEqual(1L, view.ConnectionOpenFailures);
			Assert.AreEqual(1L, view.ConnectionAuthFailures);
			Assert.AreEqual(0L, view.ConnectionTlsFailures);
		}

		[TestMethod]
		public void MetricsAsyncOpeningTimeoutAndReadyAreExclusive()
		{
			AsyncCluster cluster = GetAsyncCluster();
			MetricsSession timeoutSession = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			TestAsyncOpeningCommand timeoutFirst =
				new(cluster, timeoutSession, (AsyncNode)cluster.Nodes[0]);

			timeoutFirst.ExpireTotalTimeout();
			Assert.IsFalse(timeoutFirst.CheckTimeout());
			timeoutFirst.Ready();

			MetricsSessionView timeoutView = timeoutSession.Read();
			Assert.AreEqual(1L, timeoutView.ConnectionOpenFailures);
			Assert.AreEqual(0L, timeoutView.ConnectionsOpened);
			Assert.AreEqual(0, timeoutFirst.WriteCount);

			MetricsSession readySession = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			TestAsyncOpeningCommand readyFirst =
				new(cluster, readySession, (AsyncNode)cluster.Nodes[0]);

			readyFirst.Ready();
			readyFirst.Ready();

			MetricsSessionView readyView = readySession.Read();
			Assert.AreEqual(0L, readyView.ConnectionOpenFailures);
			Assert.AreEqual(1L, readyView.ConnectionsOpened);
			Assert.AreEqual(1, readyFirst.WriteCount);
		}

		[TestMethod]
		public void MetricsRecoveryCountsAdmissionAndTerminalOnce()
		{
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			RecoverMetricsHandle handle = new(session);

			Assert.AreEqual(0L, session.Read().RecoverEnqueued);
			handle.Enqueue();
			handle.Enqueue();
			handle.Abort();
			handle.Abort();

			MetricsSessionView view = session.Read();
			Assert.AreEqual(1L, view.RecoverEnqueued);
			Assert.AreEqual(0L, view.RecoverCompleted);
			Assert.AreEqual(1L, view.RecoverAborted);
		}

		[TestMethod]
		public void MetricsLegacySlotDisposesAfterLastLease()
		{
			NodeMetrics metrics = new(CreateInternalPolicy());
			NodeMetricsSlot slot = new(metrics);
			NodeMetricsLease lease = slot.Acquire();

			slot.Retire();
			Assert.IsNotNull(metrics.BytesInCounter);
			NodeMetricsLease retiredLease = slot.Acquire();
			Assert.IsNull(retiredLease.Metrics);
			retiredLease.Dispose();

			lease.Dispose();
			Assert.IsNull(metrics.BytesInCounter);
			Assert.IsNull(metrics.BytesOutCounter);
			Assert.IsNull(metrics.Histograms);
			slot.Retire();
		}

		[TestMethod]
		public void MetricsPointOwnershipTransfersExactlyOnce()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			CommandMetricsHandle point = session.BeginApiCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ);
			ApiMetricsContext context =
				new(client.Cluster, session, session.RuntimeConfig, point);

			CommandMetricsHandle command = context.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			Assert.IsTrue(command.Enabled);
			Assert.IsFalse(context.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true).Enabled);

			context.AbandonPointOwner();
			command.CompleteSuccess(node);
			Assert.AreEqual(
				1L,
				GetNamespace(session.Read(), node)
					.Latency[Latency.LatencyType.READ].Sum());
		}

		[TestMethod]
		public void MetricsBatchPartialFailureRecordsOnFirstTransition()
		{
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			BatchStatus status = new(true);
			status.SetCommandMetrics(session.BeginApiCoordinator());

			status.SetRowError();
			Assert.AreEqual(1L, session.Read().BatchPartialFailures);
			status.SetRowError();
			Assert.AreEqual(1L, session.Read().BatchPartialFailures);
		}

		[TestMethod]
		public void MetricsBatchPartialFailureHonorsSamplingAndEarlyTransition()
		{
			MetricsPolicy rejectedPolicy = CreateInternalPolicy();
			rejectedPolicy.Sampler = new MetricsSampler { Range = 1, Threshold = 0 };
			MetricsSession rejected = new(
				rejectedPolicy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			BatchStatus rejectedStatus = new(true);
			rejectedStatus.SetCommandMetrics(rejected.BeginApiCoordinator());
			rejectedStatus.SetRowError();
			Assert.AreEqual(0L, rejected.Read().BatchPartialFailures);

			MetricsSession sampled = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			BatchStatus earlyStatus = new(true);
			earlyStatus.SetRowError();
			earlyStatus.SetCommandMetrics(sampled.BeginApiCoordinator());
			Assert.AreEqual(1L, sampled.Read().BatchPartialFailures);
		}

		[TestMethod]
		public void MetricsCommandClassifiesTerminalFailures()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0, 0, 0, 0, 0, 0));

			CompleteFailure(session, node, new AerospikeException.Timeout(new Policy(), true), true);
			CompleteFailure(session, node, new AerospikeException.Timeout(new Policy(), false), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.KEY_BUSY), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.RECORD_TOO_BIG), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.DEVICE_OVERLOAD), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.PARAMETER_ERROR), false);

			MetricsSessionView view = session.Read();
			MetricsNamespaceView ns = GetNamespace(view, node);
			Assert.AreEqual(1L, view.ClientTimeouts);
			Assert.AreEqual(1L, view.ServerTimeouts);
			Assert.AreEqual(2L, ns.Timeouts);
			Assert.AreEqual(1L, ns.KeyBusy);
			Assert.AreEqual(1L, ns.RecordTooBig);
			Assert.AreEqual(1L, ns.DeviceOverload);
			Assert.AreEqual(1L, ns.Errors);
		}

		[TestMethod]
		public void MetricsFrozenSessionRejectsTerminalWrites()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.WRITE,
				true);
			handle.AddBytes(node, 5, 7);

			session.Freeze();
			handle.CompleteSuccess(node);

			MetricsSessionView view = session.Read();
			Assert.IsFalse(view.Active);
			Assert.AreEqual(1L, view.CommandCount);
			Assert.AreEqual(0, view.Nodes.Count);
		}

		[TestMethod]
		public void MetricsFreezePreservesAlreadyAdmittedWrite()
		{
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			MetricsWritePermit permit = session.AcquireWrite();
			Task freezeTask = null;

			try
			{
				Assert.IsTrue(permit.Acquired);
				freezeTask = Task.Run(session.Freeze);
				Assert.IsTrue(SpinWait.SpinUntil(
					() => !session.AcceptsWrites,
					TimeSpan.FromSeconds(1)));
				Assert.IsFalse(freezeTask.IsCompleted);

				session.AddTimeoutAdmitted(true);
			}
			finally
			{
				permit.Dispose();
			}

			Assert.IsTrue(freezeTask.Wait(TimeSpan.FromSeconds(1)));
			Assert.AreEqual(1L, session.Read().ClientTimeouts);
		}

		[TestMethod]
		public void MetricsPauseWaitsForAdmittedBegin()
		{
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			MetricsBeginPermit permit = session.AcquireBegin();
			Task<bool> pauseTask = null;

			try
			{
				Assert.IsTrue(permit.Acquired);
				pauseTask = Task.Run(session.Pause);
				Assert.IsTrue(SpinWait.SpinUntil(
					() => !session.CanBegin,
					TimeSpan.FromSeconds(1)));
				Assert.IsFalse(pauseTask.IsCompleted);
			}
			finally
			{
				permit.Dispose();
			}

			Assert.IsTrue(pauseTask.Result);
			session.Resume();
		}

		[TestMethod]
		public void MetricsInFlightCommandCompletesAfterOperationalDisable()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			MetricsSession session = new(
				policy,
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			handle.AddBytes(node, 7, 11);

			session.ApplyPolicy(new MetricsPolicy(policy)
			{
				OperationalEnabled = false
			});
			handle.CompleteSuccess(node);

			MetricsSessionView view = session.Read();
			MetricsNamespaceView ns = GetNamespace(view, node);
			Assert.AreEqual(1L, view.CommandCount);
			Assert.AreEqual(7L, ns.BytesIn);
			Assert.AreEqual(11L, ns.BytesOut);
			Assert.AreEqual(1L, ns.Latency[Latency.LatencyType.READ].Sum());
		}

		[TestMethod]
		public void MetricsInFlightConnectionsCompleteAfterOperationalDisable()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			MetricsSession session = new(
				policy,
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom());
			Assert.IsTrue(session.TryStartConnection(
				out long successStart,
				out MetricsRuntimeConfig successConfig));
			ConnectionMetricsHandle success = new(
				session,
				successConfig,
				successStart);
			Assert.IsTrue(session.TryStartConnection(
				out long failureStart,
				out MetricsRuntimeConfig failureConfig));
			ConnectionMetricsHandle failure = new(
				session,
				failureConfig,
				failureStart);

			session.ApplyPolicy(new MetricsPolicy(policy)
			{
				OperationalEnabled = false
			});
			success.Complete(node, SuiteHelpers.ns);
			failure.Fail(ConnectionFailurePhase.Auth);

			MetricsSessionView view = session.Read();
			Assert.AreEqual(1L, view.ConnectionsOpened);
			Assert.AreEqual(1L, view.ConnectionOpenFailures);
			Assert.AreEqual(1L, view.ConnectionAuthFailures);
			Assert.AreEqual(
				1L,
				GetNamespace(view, node)
					.Latency[Latency.LatencyType.CONN].Sum());
		}

		[TestMethod]
		public void MetricsInFlightRecoveryCompletesAfterOperationalDisable()
		{
			MetricsPolicy policy = CreateInternalPolicy();
			MetricsSession session = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			Assert.IsTrue(session.TryStartRecover());
			RecoverMetricsHandle handle = new(session);
			handle.Enqueue();

			session.ApplyPolicy(new MetricsPolicy(policy)
			{
				OperationalEnabled = false
			});
			handle.Complete();

			MetricsSessionView view = session.Read();
			Assert.AreEqual(1L, view.RecoverEnqueued);
			Assert.AreEqual(1L, view.RecoverCompleted);
		}

		[TestMethod]
		public void MetricsInFlightTendCompletesAfterOperationalDisable()
		{
			MetricsPolicy policy = CreateInternalPolicy();
			MetricsSession session = new(
				policy,
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom());
			long startTimestamp = session.StartTend();

			session.ApplyPolicy(new MetricsPolicy(policy)
			{
				OperationalEnabled = false
			});
			session.CompleteTend(startTimestamp, 0);

			Assert.AreEqual(1L, session.Read().TendDuration.Sum());
		}

		[TestMethod]
		public void MetricsRemovedNodeRetainsCollectedHistory()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			session.AddConnectionOpened(node);
			session.RemoveNode(node);

			MetricsSessionView view = session.Read();
			Assert.AreEqual(0, view.Nodes.Count);
			Assert.AreEqual(1L, view.RemovedNodes.Opened);
		}

		[TestMethod]
		public void MetricsFirstNodeWriteCannotRaceDepartureOwnership()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			using ManualResetEventSlim collectorResolved = new(false);
			using ManualResetEventSlim installDeparture = new(false);

			try
			{
				Task<MetricsNodeDeparture> departureTask = Task.Run(() =>
					session.BeginNodeDeparture(
						node,
						() =>
						{
							collectorResolved.Set();
							installDeparture.Wait();
						}));

				Assert.IsTrue(collectorResolved.Wait(TimeSpan.FromSeconds(5)));
				session.AddConnectionOpened(node);
				installDeparture.Set();
				MetricsNodeDeparture departure = departureTask.GetAwaiter().GetResult();

				Assert.AreEqual(
					1L,
					departure.Collector.Read(
						node.Name,
						session.RuntimeConfig).Opened);
			}
			finally
			{
				installDeparture.Set();
				node.InstallMetricsDeparture(null);
			}
		}

		[TestMethod]
		public void MetricsRecordNotFoundCommitsSuccessEquivalentData()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsSession session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			handle.AddBytes(node, 11, 13);
			handle.CompleteFailure(
				node,
				new AerospikeException(ResultCode.KEY_NOT_FOUND_ERROR),
				false);

			MetricsNamespaceView ns = GetNamespace(session.Read(), node);
			Assert.AreEqual(11L, ns.BytesIn);
			Assert.AreEqual(13L, ns.BytesOut);
			Assert.AreEqual(0L, ns.Errors);
			Assert.AreEqual(1L, ns.Latency[Latency.LatencyType.READ].Sum());
		}

		[TestMethod]
		public void MetricsBatchNodeOnlyAttributesHomogeneousNamespace()
		{
			Node node = client.Cluster.Nodes[0];
			Key[] homogeneous =
			[
				new Key(SuiteHelpers.ns, SuiteHelpers.set, 1),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, 2)
			];
			Key[] mixed =
			[
				new Key(SuiteHelpers.ns, SuiteHelpers.set, 1),
				new Key(SuiteHelpers.ns + "-other", SuiteHelpers.set, 2)
			];

			Assert.AreEqual(SuiteHelpers.ns, new BatchNode(node, homogeneous).ns);
			Assert.IsNull(new BatchNode(node, mixed).ns);
		}

		[TestMethod]
		public void MetricsReadViewsAreImmutableAndSessionIdentified()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsSession first = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			CommandMetricsHandle handle = first.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			handle.CompleteSuccess(node);
			MetricsSessionView view = first.Read();
			MetricsNamespaceView ns = GetNamespace(view, node);

			AssertThrows<NotSupportedException>(() =>
				((IDictionary<string, MetricsNodeView>)view.Nodes).Add("x", view.Nodes[node.Name]));
			AssertThrows<NotSupportedException>(() =>
				((IList<long>)ns.Latency[Latency.LatencyType.READ])[0] = 99);

			MetricsSession second = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			Assert.AreNotEqual(view.SessionId, second.Read().SessionId);
		}

		[TestMethod]
		public void MetricsTierZeroConnectionDoesNotStartOperationalTimer()
		{
			Node node = client.Cluster.Nodes[0];
			TestMetricsClock clock = new();
			MetricsPolicy policy = CreateInternalPolicy();
			policy.OperationalEnabled = false;
			MetricsSession session = new(policy, clock, new TestMetricsRandom());
			ConnectionMetricsHandle handle =
				new(session, session.StartConnection());

			handle.Complete(node, null);

			MetricsSessionView view = session.Read();
			Assert.AreEqual(0, clock.TimestampCallCount);
			Assert.AreEqual(1L, view.ConnectionsOpened);
			Assert.AreEqual(1L, view.Nodes[node.Name].Opened);
			Assert.AreEqual(0, view.Nodes[node.Name].Namespaces.Count);
		}

		[TestMethod]
		public void MetricsTierZeroCloseSkipsOperationalReason()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			policy.OperationalEnabled = false;
			MetricsSession session = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());

			session.AddConnectionOpened(node);
			session.AddConnectionClosed(node, ConnectionCloseReason.Error);

			MetricsNodeView nodeView = session.Read().Nodes[node.Name];
			Assert.AreEqual(1L, nodeView.Opened);
			Assert.AreEqual(1L, nodeView.Closed);
			Assert.AreEqual(0L, nodeView.ClosedError);
		}

		[TestMethod]
		public void MetricsTierZeroSkipsCommandCollection()
		{
			MetricsPolicy policy = CreateInternalPolicy();
			policy.OperationalEnabled = false;
			client.EnableMetrics(policy);

			try
			{
				int legacyCommandCount = client.Cluster.GetCommandCount();
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-v3-tier-zero-key");
				client.Put(null, key, new Bin("m", 1));
				client.Get(null, key);

				MetricsSessionView view = client.Cluster.ReadMetrics();
				Assert.AreEqual(0L, view.CommandCount);
				Assert.AreEqual(2L, view.ApiBlocking);
				Assert.AreEqual(legacyCommandCount, client.Cluster.GetCommandCount());
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public async Task MetricsCollectsSyncAndAsyncPointCommands()
		{
			MetricsPolicy syncPolicy = CreateInternalPolicy();
			client.EnableMetrics(syncPolicy);

			try
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-v3-sync-key");
				client.Put(null, key, new Bin("m", 1));
				client.Get(null, key);

				AssertPointMetrics(
					client.Cluster.ReadMetrics(),
					ApiExecutionMode.Blocking);
			}
			finally
			{
				client.DisableMetrics();
			}

			AsyncClient asyncClient = (AsyncClient)SuiteHelpers.asyncClient;
			MetricsPolicy asyncPolicy = CreateInternalPolicy();
			asyncClient.EnableMetrics(asyncPolicy);

			try
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-v3-async-key");
				await asyncClient.Put(null, CancellationToken.None, key, new Bin("m", 1));
				await asyncClient.Get(null, CancellationToken.None, key);

				AssertPointMetrics(
					asyncClient.Cluster.ReadMetrics(),
					ApiExecutionMode.Deferred);
			}
			finally
			{
				asyncClient.DisableMetrics();
			}
		}

		[TestMethod]
		public async Task MetricsCountsEmptySyncAndAsyncBatchCalls()
		{
			client.EnableMetrics(CreateInternalPolicy());

			try
			{
				client.Get(null, Array.Empty<Key>());

				MetricsSessionView view = client.Cluster.ReadMetrics();
				Assert.AreEqual(1L, view.CommandCount);
				Assert.AreEqual(1L, view.ApiBlocking);
				Assert.AreEqual(0, view.Nodes.Count);
			}
			finally
			{
				client.DisableMetrics();
			}

			AsyncClient asyncClient = (AsyncClient)SuiteHelpers.asyncClient;
			asyncClient.EnableMetrics(CreateInternalPolicy());

			try
			{
				await asyncClient.Get(null, CancellationToken.None, Array.Empty<Key>());

				MetricsSessionView view = asyncClient.Cluster.ReadMetrics();
				Assert.AreEqual(1L, view.CommandCount);
				Assert.AreEqual(1L, view.ApiDeferred);
				Assert.AreEqual(0, view.Nodes.Count);
			}
			finally
			{
				asyncClient.DisableMetrics();
			}
		}

		[TestMethod]
		public async Task MetricsAsyncBatchExpectedMissIsNotPartialFailure()
		{
			AsyncClient asyncClient = (AsyncClient)SuiteHelpers.asyncClient;
			asyncClient.EnableMetrics(CreateInternalPolicy());

			try
			{
				Key key = new(
					SuiteHelpers.ns,
					SuiteHelpers.set,
					$"metrics-v3-missing-{Guid.NewGuid():N}");
				Key[] keys = [key];

				Record[] records = await asyncClient.Get(
					null,
					CancellationToken.None,
					keys);
				bool[] exists = await asyncClient.Exists(
					null,
					CancellationToken.None,
					keys);

				Assert.IsNull(records[0]);
				Assert.IsFalse(exists[0]);
				Assert.AreEqual(
					0L,
					asyncClient.Cluster.ReadMetrics().BatchPartialFailures);
			}
			finally
			{
				asyncClient.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsPolicyRejectsInvalidSamplerAtomically()
		{
			MetricsPolicy policy = new()
			{
				Listener = new CapturingMetricsListener(),
				Sampler = new MetricsSampler
				{
					Range = 0,
					Threshold = 0
				}
			};

			AssertThrows<AerospikeException>(() => client.EnableMetrics(policy));
			Assert.IsFalse(client.Cluster.MetricsEnabled);
		}

		[TestMethod]
		public void MetricsListenerEnableFailureRestoresPreviousSession()
		{
			CapturingMetricsListener listener = new();
			MetricsPolicy policy = new() { Listener = listener };
			client.EnableMetrics(policy);

			try
			{
				AssertThrows<InvalidOperationException>(() =>
					client.EnableMetrics(new MetricsPolicy { Listener = new FailingMetricsListener() }));
				Assert.IsTrue(client.Cluster.MetricsEnabled);
				Assert.AreSame(listener, client.Cluster.MetricsPolicy.Listener);
				Assert.AreEqual(2, listener.EnableCount);
				Assert.AreEqual(1, listener.DisableCount);
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsListenerEnableFailureCleansCandidate()
		{
			CapturingMetricsListener previous = new();
			FailingMetricsListener candidate = new();
			client.EnableMetrics(new MetricsPolicy { Listener = previous });

			try
			{
				AssertThrows<InvalidOperationException>(() =>
					client.EnableMetrics(new MetricsPolicy { Listener = candidate }));
				Assert.AreEqual(1, candidate.DisableCount);
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsListenerDisableFailureRestoresPreviousSession()
		{
			FailingDisableOnceMetricsListener listener = new();
			client.EnableMetrics(new MetricsPolicy { Listener = listener });

			try
			{
				Key first = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-disable-rollback-1");
				Key second = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-disable-rollback-2");

				client.Put(null, first, new Bin("m", 1));
				AssertThrows<InvalidOperationException>(() => client.DisableMetrics());

				Assert.IsTrue(client.Cluster.MetricsEnabled);
				Assert.IsTrue(client.Cluster.ReadMetrics().Active);

				client.Put(null, second, new Bin("m", 2));
				Assert.AreEqual(2L, client.Cluster.ReadMetrics().CommandCount);
			}
			finally
			{
				if (client.Cluster.MetricsEnabled)
				{
					client.DisableMetrics();
				}
			}
			Assert.IsFalse(client.Cluster.MetricsEnabled);
		}

		[TestMethod]
		public void MetricsListenerCallbacks()
		{
			CapturingMetricsListener listener = new();
			MetricsPolicy policy = new()
			{
				Listener = listener,
				Interval = 1
			};

			client.EnableMetrics(policy);
			try
			{
				Assert.IsTrue(client.Cluster.MetricsEnabled);
				Assert.IsTrue(listener.EnableCount >= 1);

				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-listener-key");
				client.Put(null, key, new Bin("m", 1));
				client.Get(null, key);

				WaitForSnapshots(listener, minCount: 1);
			}
			finally
			{
				client.DisableMetrics();
			}

			Assert.IsTrue(listener.DisableCount >= 1);
			Assert.IsFalse(client.Cluster.MetricsEnabled);
		}

		[TestMethod]
		public void MetricsWriterWritesFile()
		{
			string reportDir = Path.Combine(Path.GetTempPath(), $"as-metrics-writer-{Guid.NewGuid():N}");
			Directory.CreateDirectory(reportDir);

			try
			{
				MetricsPolicy policy = new()
				{
					ReportDir = reportDir,
					Interval = 1
				};

				client.EnableMetrics(policy);
				try
				{
					Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-writer-key");
					client.Put(null, key, new Bin("m", 1));
					client.Get(null, key);

					Util.Sleep(2500);
				}
				finally
				{
					client.DisableMetrics();
				}

				string[] files = Directory.GetFiles(reportDir, "metrics-*.log");
				Assert.IsTrue(files.Length > 0, "Expected at least one metrics log file");

				string content = File.ReadAllText(files[0]);
				Assert.IsTrue(content.Contains("header(2)"), "Metrics file should contain header");
				Assert.IsTrue(content.Contains("cluster["), "Metrics file should contain cluster snapshot");
			}
			finally
			{
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		[TestMethod]
		public void MetricsWriterRestartDoesNotTruncateSameTimestampFile()
		{
			string reportDir = Path.Combine(
				Path.GetTempPath(),
				$"as-metrics-writer-restart-{Guid.NewGuid():N}");
			Directory.CreateDirectory(reportDir);
			MetricsPolicy policy = new()
			{
				ReportDir = reportDir,
				Interval = 1
			};

			try
			{
				client.EnableMetrics(policy);
				client.DisableMetrics();
				client.EnableMetrics(policy);
				client.DisableMetrics();

				string[] files = Directory.GetFiles(reportDir, "metrics-*.log");
				Assert.AreEqual(2, files.Length);

				foreach (string file in files)
				{
					Assert.IsTrue(File.ReadAllText(file).Contains("header(2)"));
				}
			}
			finally
			{
				if (client.Cluster.MetricsEnabled)
				{
					client.DisableMetrics();
				}
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		[TestMethod]
		public void ConfigMergesMetricsPolicyWithoutEnableField()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.GoodConfigYaml);
			AerospikeClient configClient = scope.Client;

			CapturingMetricsListener listener = new();
			MetricsPolicy policy = new()
			{
				Listener = listener,
				Interval = 1
			};

			configClient.EnableMetrics(policy);
			try
			{
				Assert.IsTrue(configClient.Cluster.MetricsEnabled);
				Assert.AreEqual(2, configClient.Cluster.MetricsPolicy.LatencyShift);
				Assert.AreEqual(7, configClient.Cluster.MetricsPolicy.LatencyColumns);
				Assert.IsNotNull(configClient.Cluster.MetricsPolicy.labels);
				Assert.AreEqual("us-west", configClient.Cluster.MetricsPolicy.labels["region"]);
			}
			finally
			{
				configClient.DisableMetrics();
			}
		}

		[TestMethod]
		public void ConfigRejectsManualEnableWhenDisabledInYaml()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.MetricsDisabledYaml);
			AerospikeClient configClient = scope.Client;

			CapturingMetricsListener listener = new();
			configClient.EnableMetrics(new MetricsPolicy { Listener = listener, Interval = 1 });

			Assert.IsFalse(configClient.Cluster.MetricsEnabled);
			Assert.AreEqual(0, listener.EnableCount);
		}

		[TestMethod]
		public void GetClusterStatsWithMetricsEnabled()
		{
			CapturingMetricsListener listener = new();
			client.EnableMetrics(new MetricsPolicy { Listener = listener, Interval = 1 });
			try
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-stats-key");
				client.Put(null, key, new Bin("m", 1));
				client.Get(null, key);

				ClusterStats stats = client.GetClusterStats();
				Assert.IsNotNull(stats);
				Assert.IsTrue(stats.nodes.Length > 0);
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		private static void WaitForSnapshots(CapturingMetricsListener listener, int minCount)
		{
			for (int i = 0; i < 30; i++)
			{
				if (listener.SnapshotCount >= minCount)
				{
					return;
				}
				Util.Sleep(500);
			}

			Assert.Fail($"Expected at least {minCount} metrics snapshot(s), received {listener.SnapshotCount}");
		}

		private static MetricsPolicy CreateInternalPolicy()
		{
			return new MetricsPolicy
			{
				Listener = new CapturingMetricsListener(),
				OperationalEnabled = true,
				UsageEnabled = true,
				Sampler = new MetricsSampler()
			};
		}

		private static AsyncCluster GetAsyncCluster()
		{
			FieldInfo field = typeof(AsyncClient).GetField(
				"cluster",
				BindingFlags.Instance | BindingFlags.NonPublic);
			return (AsyncCluster)field.GetValue(SuiteHelpers.asyncClient);
		}

		private static MetricsNamespaceView GetNamespace(MetricsSessionView view, Node node)
		{
			return view.Nodes[node.Name].Namespaces[SuiteHelpers.ns];
		}

		private static void CompleteFailure(
			MetricsSession session,
			Node node,
			AerospikeException exception,
			bool clientTimeout)
		{
			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			handle.CompleteFailure(node, exception, clientTimeout);
		}

		private static void AssertPointMetrics(
			MetricsSessionView view,
			ApiExecutionMode expectedMode)
		{
			Assert.IsNotNull(view);
			Assert.AreEqual(2L, view.CommandCount);
			Assert.AreEqual(
				expectedMode == ApiExecutionMode.Blocking ? 2L : 0L,
				view.ApiBlocking);
			Assert.AreEqual(
				expectedMode == ApiExecutionMode.Deferred ? 2L : 0L,
				view.ApiDeferred);

			long bytesIn = 0;
			long bytesOut = 0;
			long reads = 0;
			long writes = 0;

			foreach (MetricsNodeView node in view.Nodes.Values)
			{
				if (node.Namespaces.TryGetValue(SuiteHelpers.ns, out MetricsNamespaceView ns))
				{
					bytesIn += ns.BytesIn;
					bytesOut += ns.BytesOut;
					reads += ns.Latency[Latency.LatencyType.READ].Sum();
					writes += ns.Latency[Latency.LatencyType.WRITE].Sum();
				}
			}

			Assert.IsTrue(bytesIn > 0);
			Assert.IsTrue(bytesOut > 0);
			Assert.AreEqual(1L, reads);
			Assert.AreEqual(1L, writes);
		}

		private static void AssertThrows<T>(Action action) where T : Exception
		{
			try
			{
				action();
			}
			catch (T)
			{
				return;
			}
			catch (Exception e)
			{
				Assert.Fail($"Expected {typeof(T).Name}, received {e.GetType().Name}: {e.Message}");
			}

			Assert.Fail($"Expected {typeof(T).Name}");
		}

		private sealed class CapturingMetricsListener : IMetricsListener
		{
			public int EnableCount { get; private set; }
			public int SnapshotCount { get; private set; }
			public int DisableCount { get; private set; }

			public void OnEnable(Cluster cluster, MetricsPolicy policy) => EnableCount++;

			public void OnSnapshot(Cluster cluster) => SnapshotCount++;

			public void OnNodeClose(Node node) { }

			public void OnDisable(Cluster cluster) => DisableCount++;
		}

		private sealed class FailingMetricsListener : IMetricsListener
		{
			public int DisableCount { get; private set; }

			public void OnEnable(Cluster cluster, MetricsPolicy policy) =>
				throw new InvalidOperationException("Expected metrics listener failure");

			public void OnSnapshot(Cluster cluster) { }

			public void OnNodeClose(Node node) { }

			public void OnDisable(Cluster cluster) => DisableCount++;
		}

		private sealed class FailingDisableOnceMetricsListener : IMetricsListener
		{
			private int disableCount;

			public void OnEnable(Cluster cluster, MetricsPolicy policy) { }

			public void OnSnapshot(Cluster cluster) { }

			public void OnNodeClose(Node node) { }

			public void OnDisable(Cluster cluster)
			{
				if (Interlocked.Increment(ref disableCount) == 1)
				{
					throw new InvalidOperationException("Expected metrics listener disable failure");
				}
			}
		}

		private sealed class TestAsyncOpeningCommand : AsyncCommand
		{
			private static readonly FieldInfo ConnectionMetricsField =
				typeof(AsyncCommand).GetField(
					"connectionMetrics",
					BindingFlags.Instance | BindingFlags.NonPublic);
			private static readonly FieldInfo OpeningConnectionField =
				typeof(AsyncCommand).GetField(
					"openingConnection",
					BindingFlags.Instance | BindingFlags.NonPublic);
			private static readonly FieldInfo TotalWatchField =
				typeof(AsyncCommand).GetField(
					"totalWatch",
					BindingFlags.Instance | BindingFlags.NonPublic);
			private static readonly MethodInfo ConnectionReadyMethod =
				typeof(AsyncCommand).GetMethod(
					"ConnectionReady",
					BindingFlags.Instance | BindingFlags.NonPublic);

			internal int WriteCount { get; private set; }

			internal TestAsyncOpeningCommand(
				AsyncCluster cluster,
				MetricsSession session,
				AsyncNode node)
				: base(
					cluster,
					new Policy { totalTimeout = 0 },
					SuiteHelpers.ns)
			{
				this.node = node;
				ConnectionMetricsField.SetValue(
					this,
					new ConnectionMetricsHandle(session, session.StartConnection()));
				OpeningConnectionField.SetValue(this, 1);
			}

			internal void ExpireTotalTimeout()
			{
				TotalWatchField.SetValue(this, ValueStopwatch.StartNew());
			}

			internal void Ready()
			{
				ConnectionReadyMethod.Invoke(this, null);
			}

			protected internal override Node GetNode(Cluster cluster) => node;

			protected override Latency.LatencyType GetLatencyType() =>
				Latency.LatencyType.READ;

			protected internal override void WriteBuffer()
			{
				WriteCount++;
			}

			protected internal override AsyncCommand CloneCommand() => null;

			protected internal override void ParseCommand() { }

			protected internal override bool PrepareRetry(bool timeout) => false;

			protected internal override void OnSuccess() { }

			protected internal override void OnFailure(AerospikeException ae) { }
		}

		private sealed class TestMetricsClock : IMetricsClock
		{
			internal int TimestampCallCount { get; private set; }
			internal long ElapsedNanoseconds { get; init; } = 1_000_000;

			public long GetTimestamp()
			{
				TimestampCallCount++;
				return TimestampCallCount;
			}

			public long GetElapsedNanoseconds(long startTimestamp)
			{
				return ElapsedNanoseconds;
			}
		}

		private sealed class TestMetricsRandom : IMetricsRandom
		{
			private readonly Queue<ulong> values;

			internal int CallCount { get; private set; }

			internal TestMetricsRandom(params ulong[] values)
			{
				this.values = new Queue<ulong>(values);
			}

			public ulong NextUInt64()
			{
				CallCount++;
				return values.Count == 0 ? 0 : values.Dequeue();
			}
		}
	}
}
