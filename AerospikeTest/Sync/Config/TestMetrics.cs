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
			MetricsStore session = new(policy, clock, random);

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
			Assert.AreEqual(0, clock.TimestampCallCount);
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
			MetricsStore session = new(policy, clock, random);

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
			MetricsStore session = new(policy, clock, random);

			Assert.IsTrue(session.CountCommand());
			Assert.IsTrue(session.BeginSampledCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ).Enabled);
			Assert.IsTrue(session.BeginSampledCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.BATCH).Enabled);

			Assert.AreEqual(1, random.CallCount);
			Assert.AreEqual(0, clock.TimestampCallCount);
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
			MetricsStore session = new(policy, clock, new TestMetricsRandom(0));
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
			MetricsStore session = new(
				policy,
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			CommandMetricsHandle handle = session.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			handle.CompleteSuccess(node);
			MetricsStoreSnapshot before = session.Read();

			MetricsPolicy replacement = new(policy)
			{
				UsageEnabled = !policy.UsageEnabled,
				labels = new Dictionary<string, string> { ["region"] = "test" }
			};
			session.ApplyPolicy(replacement);
			MetricsStoreSnapshot after = session.Read();

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
		public void MetricsPolicyReshapeIsEagerBeforeFirstRead()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			policy.LatencyColumns = 4;
			MetricsStore store = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			store.AddLatency(
				node,
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				1_000_000);
			MetricsHistogramShape before = store.RuntimeConfig.Shape;
			MetricsTestState.AssertShape(store, before);

			store.ApplyPolicy(new MetricsPolicy(policy) { LatencyColumns = 5 });

			MetricsHistogramShape after = store.RuntimeConfig.Shape;
			Assert.AreNotSame(before, after);
			Assert.AreEqual(before.Generation + 1, after.Generation);
			Assert.AreEqual(5, after.Columns);
			Assert.AreEqual(1, MetricsTestState.Generations(store).Count);
			MetricsTestState.AssertShape(store, after);
		}

		[TestMethod]
		public void MetricsSnapshotReadsDoNotMutateHistogramState()
		{
			client.DisableMetrics();
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			policy.LatencyColumns = 4;
			MetricsStore store = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			store.AddLatency(
				node,
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				1_000_000);
			store.CompleteTend(store.StartTend(), 0);
			store.ApplyPolicy(new MetricsPolicy(policy) { LatencyColumns = 5 });
			store.AddLatency(
				node,
				SuiteHelpers.ns,
				Latency.LatencyType.WRITE,
				1_000_000);
			IReadOnlyList<object> identities = MetricsTestState.Identities(store);
			MetricsHistogramShape shape = store.RuntimeConfig.Shape;

			MetricsStoreSnapshot first = store.Read();
			MetricsStoreSnapshot second = store.Read(store.RuntimeConfig);
			store.CaptureDepartures(store.RuntimeConfig);
			MetricsStoreSnapshot third = store.Read();

			MetricsTestState.AssertSameIdentities(
				identities,
				MetricsTestState.Identities(store));
			MetricsTestState.AssertShape(store, shape);
			foreach (MetricsStoreSnapshot view in new[] { first, second, third })
			{
				Assert.AreEqual(shape.Generation, view.ShapeGeneration);
				CollectionAssert.AreEqual(
					new long[] { 1, 0, 0, 0, 0 },
					GetNamespace(view, node)
						.Latency[Latency.LatencyType.WRITE].ToArray());
				CollectionAssert.AreEqual(
					new long[5],
					view.TendDuration.ToArray());
			}
		}

		[TestMethod]
		public void MetricsSnapshotMismatchReturnsEmptyShapeWithoutMutation()
		{
			MetricsPolicy firstPolicy = CreateInternalPolicy();
			firstPolicy.LatencyColumns = 4;
			MetricsRuntimeConfig first = new(firstPolicy);
			MetricsRuntimeConfig second = new(
				new MetricsPolicy(firstPolicy) { LatencyColumns = 6 },
				2,
				first.Shape);
			MetricsNodeCollector collector = new(first);
			collector.AddOpened();
			collector.AddConnectionLatency(1_000_000, first.Shape);
			MetricsNamespaceCollector ns =
				collector.GetNamespace(SuiteHelpers.ns, first.Shape);
			ns.AddLatency(Latency.LatencyType.READ, 1_000_000, first.Shape);
			ns.AddFailure(MetricsFailure.Error);
			object connection = MetricsTestState.Connection(collector);
			object bank = MetricsTestState.Bank(ns);

			MetricsNodeView stale = collector.Read("node", second);

			Assert.AreSame(first.Shape, collector.CurrentShape);
			Assert.AreSame(connection, MetricsTestState.Connection(collector));
			Assert.AreSame(bank, MetricsTestState.Bank(ns));
			Assert.AreEqual(1L, stale.Opened);
			CollectionAssert.AreEqual(
				new long[6],
				stale.ConnectionLatency.ToArray());
			MetricsNamespaceView staleNamespace = stale.Namespaces[SuiteHelpers.ns];
			Assert.AreEqual(1L, staleNamespace.Errors);
			foreach (IReadOnlyList<long> buckets in staleNamespace.Latency.Values)
			{
				CollectionAssert.AreEqual(new long[6], buckets.ToArray());
			}

			MetricsNodeView current = collector.Read("node", first);
			Assert.AreEqual(1L, current.ConnectionLatency.Sum());
			Assert.AreEqual(
				1L,
				current.Namespaces[SuiteHelpers.ns]
					.Latency[Latency.LatencyType.READ].Sum());
		}

		[TestMethod]
		public void MetricsShapeChangeResetsOnlyHistogramBuckets()
		{
			client.DisableMetrics();
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			policy.LatencyColumns = 4;
			MetricsStore store = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			Assert.IsTrue(store.BeginApiCall(
				ApiMetricsOperation.Read,
				ApiExecutionMode.Blocking,
				out bool sampled));
			MetricsCallContext call = new(
				client.Cluster,
				store,
				ApiMetricsOperation.Read,
				ApiExecutionMode.Blocking,
				sampled);
			CommandMetricsHandle command = call.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				false);
			command.BeginAttempt().TryFail(
				node,
				new AerospikeException.Timeout(new Policy(), true),
				true);
			command.AddRetry();
			MetricsRpcAttempt success = command.BeginAttempt();
			success.AddBytes(12, 34);
			Assert.IsTrue(success.TrySucceed(node));
			call.CompleteSuccess();
			CompleteFailure(
				store,
				node,
				new AerospikeException(ResultCode.PARAMETER_ERROR),
				false);
			new ConnectionMetricsHandle(
				store,
				store.RuntimeConfig,
				store.StartConnection(),
				ConnectionMetricsOrigin.NodePool,
				node).Complete(node, null);
			store.CompleteTend(store.StartTend(), 1);
			MetricsStoreSnapshot before = store.Read();
			Assert.AreEqual(1L, before.TendDuration.Sum());
			Assert.AreEqual(1L, before.CallLatency[Latency.LatencyType.READ].Sum());

			store.ApplyPolicy(new MetricsPolicy(policy) { LatencyColumns = 5 });
			MetricsStoreSnapshot after = store.Read();

			MetricsNodeView beforeNode = before.Nodes[node.Name];
			MetricsNodeView afterNode = after.Nodes[node.Name];
			MetricsNamespaceView beforeNamespace = GetNamespace(before, node);
			MetricsNamespaceView afterNamespace = GetNamespace(after, node);
			Assert.AreEqual(before.CommandCount, after.CommandCount);
			Assert.AreEqual(before.CommandRetries, after.CommandRetries);
			Assert.AreEqual(before.ClientTimeouts, after.ClientTimeouts);
			Assert.AreEqual(before.InvalidNodes, after.InvalidNodes);
			Assert.AreEqual(before.ConnectionsOpened, after.ConnectionsOpened);
			Assert.AreEqual(beforeNode.Opened, afterNode.Opened);
			Assert.AreEqual(beforeNamespace.Errors, afterNamespace.Errors);
			Assert.AreEqual(beforeNamespace.Timeouts, afterNamespace.Timeouts);
			Assert.AreEqual(
				beforeNamespace.ClientTimeouts,
				afterNamespace.ClientTimeouts);
			Assert.AreEqual(12L, afterNamespace.BytesIn);
			Assert.AreEqual(34L, afterNamespace.BytesOut);
			CollectionAssert.AreEqual(new long[5], after.TendDuration.ToArray());
			CollectionAssert.AreEqual(
				new long[5],
				after.CallLatency[Latency.LatencyType.READ].ToArray());
			CollectionAssert.AreEqual(
				new long[5],
				afterNode.ConnectionLatency.ToArray());
			CollectionAssert.AreEqual(
				new long[5],
				afterNamespace.Latency[Latency.LatencyType.READ].ToArray());
		}

		[TestMethod]
		public void MetricsInFlightContextsRecordOnceInNewShape()
		{
			client.DisableMetrics();
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			policy.LatencyColumns = 4;
			MetricsStore store = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			Assert.IsTrue(store.BeginApiCall(
				ApiMetricsOperation.Read,
				ApiExecutionMode.Blocking,
				out bool sampled));
			MetricsCallContext call = new(
				client.Cluster,
				store,
				ApiMetricsOperation.Read,
				ApiExecutionMode.Blocking,
				sampled);
			MetricsRpcAttempt attempt = call.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				false).BeginAttempt();
			Assert.IsTrue(store.TryStartConnection(
				out long connectionStart,
				out MetricsRuntimeConfig connectionConfig));
			ConnectionMetricsHandle connection = new(
				store,
				connectionConfig,
				connectionStart,
				ConnectionMetricsOrigin.NodePool,
				node);
			long tendStart = store.StartTend();

			store.ApplyPolicy(new MetricsPolicy(policy) { LatencyColumns = 5 });
			MetricsHistogramShape shape = store.RuntimeConfig.Shape;

			attempt.AddBytes(3, 4);
			Assert.IsTrue(attempt.TrySucceed(node));
			Assert.IsFalse(attempt.TrySucceed(node));
			connection.Complete(node, null);
			connection.Complete(node, null);
			store.CompleteTend(tendStart, 0);
			call.CompleteSuccess();

			MetricsTestState.AssertShape(store, shape);
			MetricsStoreSnapshot view = store.Read();
			MetricsNamespaceView ns = GetNamespace(view, node);
			Assert.AreEqual(shape.Generation, view.ShapeGeneration);
			Assert.AreEqual(3L, ns.BytesIn);
			Assert.AreEqual(4L, ns.BytesOut);
			CollectionAssert.AreEqual(
				new long[] { 1, 0, 0, 0, 0 },
				ns.Latency[Latency.LatencyType.READ].ToArray());
			CollectionAssert.AreEqual(
				new long[] { 1, 0, 0, 0, 0 },
				view.Nodes[node.Name].ConnectionLatency.ToArray());
			Assert.AreEqual(1L, view.ConnectionsOpened);
			CollectionAssert.AreEqual(
				new long[] { 1, 0, 0, 0, 0 },
				view.TendDuration.ToArray());
			CollectionAssert.AreEqual(
				new long[] { 1, 0, 0, 0, 0 },
				view.CallLatency[Latency.LatencyType.READ].ToArray());
		}

		[TestMethod]
		public void MetricsPolicyUpdateRollbackRestoresBucketsForward()
		{
			client.DisableMetrics();
			Node node = client.Cluster.Nodes[0];
			MetricsPolicy policy = CreateInternalPolicy();
			policy.LatencyColumns = 4;
			MetricsStore store = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			store.AddLatency(
				node,
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				1_000_000);
			store.AddFailure(node, SuiteHelpers.ns, MetricsFailure.Error);
			store.CompleteTend(store.StartTend(), 0);
			MetricsRuntimeConfig previous = store.RuntimeConfig;
			IReadOnlyList<object> identities = MetricsTestState.Identities(store);
			MetricsStoreSnapshot before = store.Read();
			MetricsPolicy reshaped = new(policy) { LatencyColumns = 5 };

			// Preparation alone publishes nothing.
			store.PreparePolicyUpdate(reshaped);
			MetricsTestState.AssertSameIdentities(
				identities,
				MetricsTestState.Identities(store));

			MetricsStore.PolicyUpdate update = store.PreparePolicyUpdate(reshaped);
			store.CommitPolicyUpdate(update);
			MetricsTestState.AssertShape(store, update.Replacement.Shape);
			store.AddLatency(
				node,
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				1_000_000);

			MetricsRuntimeConfig restored = store.RollbackPolicyUpdate(update);

			Assert.AreSame(restored, store.RuntimeConfig);
			Assert.AreEqual(
				update.Replacement.PolicyGeneration + 1,
				restored.PolicyGeneration);
			Assert.AreEqual(
				update.Replacement.Shape.Generation + 1,
				restored.Shape.Generation);
			Assert.IsTrue(restored.Shape.Matches(previous.Shape));
			MetricsTestState.AssertShape(store, restored.Shape);

			// A late write that captured the rolled-back shape cannot revert.
			MetricsNodeGeneration generation = store.RegisterNode(node);
			generation.Collector.GetNamespace(
				SuiteHelpers.ns,
				update.Replacement.Shape).AddLatency(
					Latency.LatencyType.WRITE,
					1_000_000,
					update.Replacement.Shape);
			MetricsTestState.AssertShape(store, restored.Shape);

			MetricsStoreSnapshot after = store.Read();
			MetricsNamespaceView beforeNamespace = GetNamespace(before, node);
			MetricsNamespaceView afterNamespace = GetNamespace(after, node);
			Assert.AreEqual(beforeNamespace.Errors, afterNamespace.Errors);
			CollectionAssert.AreEqual(
				beforeNamespace.Latency[Latency.LatencyType.READ].ToArray(),
				afterNamespace.Latency[Latency.LatencyType.READ].ToArray());
			CollectionAssert.AreEqual(
				new long[] { 1, 0, 0, 0 },
				afterNamespace.Latency[Latency.LatencyType.WRITE].ToArray());
			CollectionAssert.AreEqual(
				before.TendDuration.ToArray(),
				after.TendDuration.ToArray());
			Assert.AreSame(
				restored,
				store.RollbackPolicyUpdate(update));
		}

		[TestMethod]
		public void MetricsConnectionFailurePhasesAreRecordedOnce()
		{
			MetricsStore session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			ConnectionMetricsHandle handle =
				new(session, session.StartConnection());

			handle.FailPhase(ConnectionFailurePhase.Auth);
			handle.FailPhase(ConnectionFailurePhase.Auth);
			handle.Fail(ConnectionFailurePhase.Auth);
			handle.Fail(ConnectionFailurePhase.Tls);

			MetricsStoreSnapshot view = session.Read();
			Assert.AreEqual(1L, view.ConnectionOpenFailures);
			Assert.AreEqual(1L, view.ConnectionAuthFailures);
			Assert.AreEqual(0L, view.ConnectionTlsFailures);
		}

		[TestMethod]
		public void MetricsConnectionOriginsUseValidationAndNodeScopes()
		{
			Node node = client.Cluster.Nodes[0];
			TestMetricsClock clock = new();
			MetricsStore store = new(
				CreateInternalPolicy(),
				clock,
				new TestMetricsRandom());

			ConnectionMetricsHandle validation =
				new(store, store.StartConnection());
			validation.Fail(ConnectionFailurePhase.Auth);

			ConnectionMetricsHandle nodeFailure = new(
				store,
				store.RuntimeConfig,
				store.StartConnection(),
				ConnectionMetricsOrigin.NodePool,
				node);
			nodeFailure.Fail(ConnectionFailurePhase.Tls);

			ConnectionMetricsHandle nodeSuccess = new(
				store,
				store.RuntimeConfig,
				store.StartConnection(),
				ConnectionMetricsOrigin.NodePool,
				node);
			nodeSuccess.Complete(node, null);

			MetricsStoreSnapshot view = store.Read();
			MetricsNodeView nodeView = view.Nodes[node.Name];
			Assert.AreEqual(1L, view.ConnectionOpenFailures);
			Assert.AreEqual(1L, view.ConnectionAuthFailures);
			Assert.AreEqual(0L, view.ConnectionTlsFailures);
			Assert.AreEqual(1L, nodeView.OpenFailures);
			Assert.AreEqual(1L, nodeView.TlsHandshakeFailures);
			Assert.AreEqual(0L, nodeView.AuthFailures);
			Assert.AreEqual(1L, nodeView.ConnectionLatency.Sum());
			Assert.IsFalse(nodeView.Namespaces.Values.Any(
				ns => ns.Latency[Latency.LatencyType.CONN].Sum() != 0));
		}

		[TestMethod]
		public void MetricsHeterogeneousBatchAttributesDistinctNamespaces()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore store = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0, 0));
			string first = SuiteHelpers.ns;
			string second = first + "-other";
			IReadOnlyList<string> namespaces = [first, second, first];
			BatchNode batch = new(
				node,
				[
					new Key(first, SuiteHelpers.set, "metrics-batch-a"),
					new Key(second, SuiteHelpers.set, "metrics-batch-b"),
					new Key(first, SuiteHelpers.set, "metrics-batch-c")
				]);
			Assert.IsNull(batch.ns);
			CollectionAssert.AreEquivalent(
				new[] { first, second },
				batch.MetricsNamespaces.ToArray());
			CommandMetricsHandle success = store.BeginCommand(
				null,
				namespaces,
				Latency.LatencyType.BATCH,
				true);
			MetricsRpcAttempt successfulAttempt = success.BeginAttempt();
			successfulAttempt.AddBytes(12, 34);
			successfulAttempt.RecordBatchRecordFailure(
				node,
				first,
				ResultCode.PARAMETER_ERROR);
			successfulAttempt.RecordBatchRecordFailure(
				node,
				second,
				ResultCode.DEVICE_OVERLOAD);
			Assert.IsTrue(successfulAttempt.TrySucceed(node));

			CommandMetricsHandle failure = store.BeginCommand(
				null,
				namespaces,
				Latency.LatencyType.BATCH,
				true);
			Assert.IsTrue(failure.BeginAttempt().TryFail(
				node,
				new AerospikeException(ResultCode.TIMEOUT),
				true));

			MetricsStoreSnapshot view = store.Read();
			MetricsNodeView nodeView = view.Nodes[node.Name];
			MetricsNamespaceView firstView = nodeView.Namespaces[first];
			MetricsNamespaceView secondView = nodeView.Namespaces[second];
			MetricsNamespaceView unattributed = nodeView.Namespaces[string.Empty];

			Assert.AreEqual(1L, firstView.Errors);
			Assert.AreEqual(1L, secondView.DeviceOverload);
			Assert.AreEqual(1L, firstView.ClientTimeouts);
			Assert.AreEqual(1L, secondView.ClientTimeouts);
			Assert.AreEqual(1L, view.ClientTimeouts);
			Assert.AreEqual(
				1L,
				firstView.Latency[Latency.LatencyType.BATCH].Sum());
			Assert.AreEqual(
				1L,
				secondView.Latency[Latency.LatencyType.BATCH].Sum());
			Assert.AreEqual(12L, unattributed.BytesIn);
			Assert.AreEqual(34L, unattributed.BytesOut);
			Assert.AreEqual(
				0L,
				unattributed.Latency[Latency.LatencyType.BATCH].Sum());
		}

		[TestMethod]
		public void MetricsAsyncOpeningTimeoutAndReadyAreExclusive()
		{
			AsyncCluster cluster = GetAsyncCluster();
			MetricsStore timeoutSession = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			TestAsyncOpeningCommand timeoutFirst =
				new(cluster, timeoutSession, (AsyncNode)cluster.Nodes[0]);

			timeoutFirst.ExpireTotalTimeout();
			Assert.IsFalse(timeoutFirst.CheckTimeout());
			timeoutFirst.Ready();

			MetricsStoreSnapshot timeoutView = timeoutSession.Read();
			Assert.AreEqual(1L, timeoutView.ConnectionOpenFailures);
			Assert.AreEqual(0L, timeoutView.ConnectionsOpened);
			Assert.AreEqual(0, timeoutFirst.WriteCount);

			MetricsStore readySession = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			TestAsyncOpeningCommand readyFirst =
				new(cluster, readySession, (AsyncNode)cluster.Nodes[0]);

			readyFirst.Ready();
			readyFirst.Ready();

			MetricsStoreSnapshot readyView = readySession.Read();
			Assert.AreEqual(0L, readyView.ConnectionOpenFailures);
			Assert.AreEqual(1L, readyView.ConnectionsOpened);
			Assert.AreEqual(1, readyFirst.WriteCount);
		}

		[TestMethod]
		public void MetricsRecoveryCountsAdmissionAndTerminalOnce()
		{
			MetricsStore session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			RecoverMetricsHandle handle = new(session);

			Assert.AreEqual(0L, session.Read().RecoverEnqueued);
			handle.Enqueue();
			handle.Enqueue();
			handle.Abort();
			handle.Abort();

			MetricsStoreSnapshot view = session.Read();
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
		public void MetricsCallSamplingPropagatesWithoutChildDecision()
		{
			Node node = client.Cluster.Nodes[0];
			TestMetricsRandom random = new(0, 9);
			MetricsPolicy policy = CreateInternalPolicy();
			policy.Sampler = new MetricsSampler { Range = 10, Threshold = 5 };
			MetricsStore session = new(
				policy,
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				random);
			Assert.IsTrue(session.BeginApiCall(
				ApiMetricsOperation.Read,
				ApiExecutionMode.Blocking,
				out bool sampled));
			MetricsCallContext context =
				new(
					client.Cluster,
					session,
					ApiMetricsOperation.Read,
					ApiExecutionMode.Blocking,
					sampled);

			CommandMetricsHandle first = context.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			CommandMetricsHandle second = context.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);

			Assert.IsTrue(first.Enabled);
			Assert.IsTrue(second.Enabled);
			Assert.AreEqual(1, random.CallCount);
			first.BeginAttempt().TrySucceed(node);
			second.BeginAttempt().TrySucceed(node);
			Assert.AreEqual(
				2L,
				GetNamespace(session.Read(), node)
					.Latency[Latency.LatencyType.READ].Sum());
		}

		[TestMethod]
		public void MetricsBatchPartialFailureRecordsOnFirstTransition()
		{
			MetricsStore session = new(
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
			MetricsStore rejected = new(
				rejectedPolicy,
				new TestMetricsClock(),
				new TestMetricsRandom());
			BatchStatus rejectedStatus = new(true);
			rejectedStatus.SetCommandMetrics(rejected.BeginApiCoordinator());
			rejectedStatus.SetRowError();
			Assert.AreEqual(0L, rejected.Read().BatchPartialFailures);

			MetricsStore sampled = new(
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
			MetricsStore session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0, 0, 0, 0, 0, 0));

			CompleteFailure(session, node, new AerospikeException.Timeout(new Policy(), true), true);
			CompleteFailure(session, node, new AerospikeException.Timeout(new Policy(), false), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.KEY_BUSY), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.RECORD_TOO_BIG), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.DEVICE_OVERLOAD), false);
			CompleteFailure(session, node, new AerospikeException(ResultCode.PARAMETER_ERROR), false);

			MetricsStoreSnapshot view = session.Read();
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
		public void MetricsApiSamplingHasOneDecisionAndNoChildReroll()
		{
			TestMetricsClock clock = new();
			TestMetricsRandom random = new(4, 9);
			MetricsPolicy policy = CreateInternalPolicy();
			policy.Sampler = new MetricsSampler { Range = 10, Threshold = 5 };
			MetricsStore store = new(policy, clock, random);

			Assert.IsTrue(store.BeginApiCall(
				ApiMetricsOperation.Batch,
				ApiExecutionMode.Blocking,
				out bool sampled));
			MetricsCallContext call = new(
				client.Cluster,
				store,
				ApiMetricsOperation.Batch,
				ApiExecutionMode.Blocking,
				sampled);

			CommandMetricsHandle first = call.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.BATCH,
				false);
			CommandMetricsHandle retry = first.ForkRpc();

			Assert.IsTrue(first.Enabled);
			Assert.IsTrue(retry.Enabled);
			Assert.AreEqual(1, random.CallCount);
		}

		[TestMethod]
		public void MetricsZeroFullDisabledAndUninstrumentedAvoidRandom()
		{
			foreach ((ulong threshold, bool expected) in new[]
			{
				(0UL, false),
				(10UL, true)
			})
			{
				TestMetricsRandom random = new(7);
				MetricsPolicy policy = CreateInternalPolicy();
				policy.Sampler = new MetricsSampler { Range = 10, Threshold = threshold };
				MetricsStore store = new(policy, new TestMetricsClock(), random);

				Assert.IsTrue(store.BeginApiCall(
					ApiMetricsOperation.Read,
					ApiExecutionMode.Blocking,
					out bool sampled));
				Assert.AreEqual(expected, sampled);
				Assert.AreEqual(0, random.CallCount);
				Assert.IsFalse(store.BeginApiCall(
					ApiMetricsOperation.Uninstrumented,
					ApiExecutionMode.Blocking,
					out _));
				Assert.AreEqual(0, random.CallCount);

				if (!expected)
				{
					Assert.IsTrue(store.BeginApiCall(
						ApiMetricsOperation.Query,
						ApiExecutionMode.Blocking,
						out bool querySampled));
					MetricsCallContext query = new(
						client.Cluster,
						store,
						ApiMetricsOperation.Query,
						ApiExecutionMode.Blocking,
						querySampled);
					query.CompleteAbandoned();
					Assert.AreEqual(0L, store.Read().QueryAbandoned);
				}
			}

			TestMetricsRandom disabledRandom = new(0);
			MetricsPolicy disabledPolicy = CreateInternalPolicy();
			disabledPolicy.OperationalEnabled = false;
			MetricsStore disabled = new(
				disabledPolicy,
				new TestMetricsClock(),
				disabledRandom);
			Assert.IsFalse(disabled.BeginApiCall(
				ApiMetricsOperation.Query,
				ApiExecutionMode.Blocking,
				out bool disabledSampled));
			Assert.IsFalse(disabledSampled);
			Assert.AreEqual(0, disabledRandom.CallCount);
		}

		[TestMethod]
		public void MetricsRpcAttemptsRecordEachFailureAndSuccessOnlyLatency()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore store = new(
				CreateInternalPolicy(),
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			Assert.IsTrue(store.BeginApiCall(
				ApiMetricsOperation.Read,
				ApiExecutionMode.Blocking,
				out bool sampled));
			MetricsCallContext call = new(
				client.Cluster,
				store,
				ApiMetricsOperation.Read,
				ApiExecutionMode.Blocking,
				sampled);
			CommandMetricsHandle command = call.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				false);

			command.BeginAttempt().TryFail(
				node,
				new AerospikeException.Timeout(new Policy(), true),
				true);
			command.AddRetry();
			command.BeginAttempt().TryFail(
				node,
				new AerospikeException(ResultCode.PARAMETER_ERROR),
				false);
			command.AddRetry();
			command.BeginAttempt().TrySucceed(node);
			call.CompleteSuccess();

			MetricsStoreSnapshot view = store.Read();
			MetricsNamespaceView ns = GetNamespace(view, node);
			Assert.AreEqual(1L, ns.Timeouts);
			Assert.AreEqual(1L, ns.ClientTimeouts);
			Assert.AreEqual(0L, ns.ServerTimeouts);
			Assert.AreEqual(1L, ns.Errors);
			Assert.AreEqual(1L, ns.Latency[Latency.LatencyType.READ].Sum());
			Assert.AreEqual(
				1L,
				view.CallLatency[Latency.LatencyType.READ].Sum());
			Assert.AreEqual(1L, view.ClientTimeouts);
			Assert.AreEqual(2L, view.CommandRetries);
		}

		[TestMethod]
		public void MetricsRpcAttemptTerminalRaceCommitsOnce()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore store = new(
				CreateInternalPolicy(),
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			CommandMetricsHandle command = store.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			MetricsRpcAttempt attempt = command.BeginAttempt();

			Parallel.Invoke(
				() => attempt.TryFail(
					node,
					new AerospikeException.Timeout(new Policy(), true),
					true),
				() => attempt.TrySucceed(node),
				() => attempt.TryFail(
					node,
					new AerospikeException(ResultCode.PARAMETER_ERROR),
					false));

			MetricsStoreSnapshot view = store.Read();
			MetricsNamespaceView ns = GetNamespace(view, node);
			long terminals =
				ns.Timeouts +
				ns.Errors +
				ns.Latency[Latency.LatencyType.READ].Sum();
			Assert.AreEqual(1L, terminals);
			Assert.IsTrue(view.ClientTimeouts == 0 || view.ClientTimeouts == 1);
		}

		[TestMethod]
		public void MetricsBatchRecordFailureAfterAttemptTerminalIsIgnored()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore store = new(
				CreateInternalPolicy(),
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			CommandMetricsHandle command = store.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.BATCH,
				true);
			MetricsRpcAttempt attempt = command.BeginAttempt();

			Assert.IsTrue(attempt.TrySucceed(node));
			attempt.RecordBatchRecordFailure(
				node,
				SuiteHelpers.ns,
				ResultCode.KEY_BUSY);

			MetricsNamespaceView ns = GetNamespace(store.Read(), node);
			Assert.AreEqual(0L, ns.KeyBusy);
			Assert.AreEqual(0L, ns.Errors);
			Assert.AreEqual(
				1L,
				ns.Latency[Latency.LatencyType.BATCH].Sum());
		}

		[TestMethod]
		public void MetricsBatchExpectedMissIsSuccessEquivalent()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore store = new(
				CreateInternalPolicy(),
				new TestMetricsClock { ElapsedNanoseconds = 1_000_000 },
				new TestMetricsRandom(0));
			CommandMetricsHandle command = store.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.BATCH,
				true);
			MetricsRpcAttempt attempt = command.BeginAttempt();

			attempt.RecordBatchRecordFailure(
				node,
				SuiteHelpers.ns,
				ResultCode.KEY_NOT_FOUND_ERROR);
			Assert.IsTrue(attempt.TrySucceed(node));

			MetricsNamespaceView ns = GetNamespace(store.Read(), node);
			Assert.AreEqual(0L, ns.Errors);
			Assert.AreEqual(0L, ns.KeyBusy);
			Assert.AreEqual(0L, ns.DeviceOverload);
			Assert.AreEqual(
				1L,
				ns.Latency[Latency.LatencyType.BATCH].Sum());
		}

		[TestMethod]
		public void MetricsStaleAttemptKeepsLifetimeFailureOnly()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore store = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			MetricsRpcAttempt attempt = store.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true).BeginAttempt();

			store.StartSession();
			Assert.IsTrue(attempt.TryFail(
				node,
				new AerospikeException(ResultCode.PARAMETER_ERROR),
				false));
			Assert.IsFalse(attempt.TryFail(
				node,
				new AerospikeException(ResultCode.PARAMETER_ERROR),
				false));

			Assert.AreEqual(0, store.Read().Nodes.Count);
			Assert.AreEqual(1L, store.GetCompatibilityError(node));
		}

		[TestMethod]
		public void MetricsCountOnlySuppressesOperationalAttemptMetrics()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore store = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			Assert.IsTrue(store.BeginApiCall(
				ApiMetricsOperation.CountOnly,
				ApiExecutionMode.Blocking,
				out bool sampled));
			MetricsCallContext call = new(
				client.Cluster,
				store,
				ApiMetricsOperation.CountOnly,
				ApiExecutionMode.Blocking,
				sampled);
			CommandMetricsHandle command = call.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.WRITE,
				false);

			command.BeginAttempt().TryFail(
				node,
				new AerospikeException(ResultCode.PARAMETER_ERROR),
				false);
			command.AddRetry();

			MetricsStoreSnapshot view = store.Read();
			Assert.AreEqual(1L, view.CommandCount);
			Assert.AreEqual(1L, view.CommandRetries);
			Assert.AreEqual(0, view.Nodes.Count);
			Assert.AreEqual(1L, store.GetCompatibilityError(node));
		}

		[TestMethod]
		public void MetricsBlockingSetupFailureDoesNotRecordSuccessLatency()
		{
			client.DisableMetrics();
			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true,
				Sampler = MetricsSampler.FromProbability(1)
			});

			try
			{
				AssertThrows<Exception>(() => client.Exists(null, null));

				MetricsSnapshot failed = client.GetMetricsSnapshot();
				Assert.AreEqual(1L, failed.Cluster.CommandCount);
				Assert.AreEqual(0L, failed.Cluster.Latency.Read.Sum());

				client.Exists(
					null,
					new Key(SuiteHelpers.ns, SuiteHelpers.set, "metrics-setup"));

				MetricsSnapshot succeeded = client.GetMetricsSnapshot();
				Assert.AreEqual(2L, succeeded.Cluster.CommandCount);
				Assert.AreEqual(1L, succeeded.Cluster.Latency.Read.Sum());
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsCountOnlyRegisterFailureUsesCompatibilityAttempt()
		{
			client.DisableMetrics();
			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true,
				Sampler = MetricsSampler.FromProbability(1)
			});
			Node node = client.Cluster.Nodes[0];
			long errors = node.GetErrorCount();

			try
			{
				AssertThrows<AerospikeException>(() =>
					client.RegisterUdfString(
						null,
						"this is not valid lua !!!",
						"metrics-invalid.lua",
						Language.LUA));

				MetricsSnapshot snapshot = client.GetMetricsSnapshot();
				Assert.AreEqual(1L, snapshot.Cluster.CommandCount);
				Assert.AreEqual(0L, snapshot.Cluster.CommandRetries);
				Assert.IsFalse(snapshot.Nodes.Any(
					item => item.Namespaces.Count != 0));
				Assert.AreEqual(errors + 1, node.GetErrorCount());
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsRecordSetOwnsNaturalFailureAndDisposalTerminal()
		{
			TestQueryExecutor natural = new();
			using (RecordSet set = new(natural, 4, CancellationToken.None))
			{
				set.Put(RecordSet.END);
				Assert.IsFalse(set.Next());
			}
			Assert.AreEqual(1, natural.SuccessCount);
			Assert.AreEqual(0, natural.AbandonedCount);

			TestQueryExecutor failed = new()
			{
				Failure = new AerospikeException(ResultCode.PARAMETER_ERROR)
			};
			using (RecordSet set = new(failed, 4, CancellationToken.None))
			{
				set.Put(RecordSet.END);
				AssertThrows<AerospikeException>(() => set.Next());
			}
			Assert.AreEqual(1, failed.FailureCount);

			TestQueryExecutor abandoned = new();
			RecordSet abandonedSet = new(abandoned, 4, CancellationToken.None);
			abandonedSet.Close();
			abandonedSet.Dispose();
			Assert.AreEqual(1, abandoned.AbandonedCount);
			Assert.AreEqual(0, abandoned.SuccessCount);

			TestQueryExecutor aggregate = new();
			using (ResultSet set = new(
				aggregate,
				4,
				CancellationToken.None))
			{
				set.Put(ResultSet.END);
				Assert.IsFalse(set.Next());
			}
			Assert.AreEqual(1, aggregate.SuccessCount);
		}

		[TestMethod]
		public void MetricsFrozenSessionRejectsTerminalWrites()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore session = new(
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

			MetricsStoreSnapshot view = session.Read();
			Assert.IsFalse(view.Active);
			Assert.AreEqual(1L, view.CommandCount);
			Assert.AreEqual(0, view.Nodes.Count);
		}

		[TestMethod]
		public void MetricsFreezePreservesAlreadyAdmittedWrite()
		{
			MetricsStore session = new(
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
			MetricsStore session = new(
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
			MetricsStore session = new(
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

			MetricsStoreSnapshot view = session.Read();
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
			MetricsStore session = new(
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

			MetricsStoreSnapshot view = session.Read();
			Assert.AreEqual(1L, view.ConnectionsOpened);
			Assert.AreEqual(1L, view.ConnectionOpenFailures);
			Assert.AreEqual(1L, view.ConnectionAuthFailures);
			Assert.AreEqual(
				1L,
				view.Nodes[node.Name].ConnectionLatency.Sum());
		}

		[TestMethod]
		public void MetricsInFlightRecoveryCompletesAfterOperationalDisable()
		{
			MetricsPolicy policy = CreateInternalPolicy();
			MetricsStore session = new(
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

			MetricsStoreSnapshot view = session.Read();
			Assert.AreEqual(1L, view.RecoverEnqueued);
			Assert.AreEqual(1L, view.RecoverCompleted);
		}

		[TestMethod]
		public void MetricsInFlightTendCompletesAfterOperationalDisable()
		{
			MetricsPolicy policy = CreateInternalPolicy();
			MetricsStore session = new(
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
			MetricsStore session = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom());
			session.AddConnectionOpened(node);
			session.RemoveNode(node);

			MetricsStoreSnapshot view = session.Read();
			Assert.AreEqual(0, view.Nodes.Count);
			Assert.AreEqual(1L, view.RemovedNodes.Opened);
		}

		[TestMethod]
		public void MetricsNodeNameStatePreservesCumulativeRejoinAndLateWrites()
		{
			MetricsRuntimeConfig config = new(CreateInternalPolicy());
			MetricsStore store = new(CreateInternalPolicy());
			MetricsNodeNameState state = new();
			MetricsNodeGeneration first =
				new(store, "N1", config);
			MetricsNodeGeneration second =
				new(store, "N1", config);

			state.Add(first);
			state.Admit(first);
			first.Collector.AddOpened();
			state.Depart(first);

			state.Add(second);
			state.Admit(second);
			second.Collector.AddOpened();
			first.Collector.AddClosed();

			MetricsNodeView active = state.Read("N1", config);
			Assert.IsTrue(state.IsActive);
			Assert.AreEqual(2L, active.Opened);
			Assert.AreEqual(1L, active.Closed);

			state.Depart(second);
			Assert.IsFalse(state.IsActive);
			MetricsNodeView departed = state.Read("N1", config);
			Assert.AreEqual(2L, departed.Opened);
			Assert.AreEqual(1L, departed.Closed);
		}

		[TestMethod]
		public void MetricsDisposedGenerationHidesPublicCountsButRetainsHistory()
		{
			MetricsRuntimeConfig config = new(CreateInternalPolicy());
			MetricsStore store = new(CreateInternalPolicy());
			MetricsNodeGeneration generation =
				new(store, "N1", config);

			generation.AddError(SuiteHelpers.ns);
			generation.AddTimeout(SuiteHelpers.ns);
			generation.AddKeyBusy(SuiteHelpers.ns);
			generation.Collector.AddOpened();

			Assert.AreEqual(1L, generation.GetErrorCount());
			Assert.AreEqual(1L, generation.GetTimeoutCount());
			Assert.AreEqual(1L, generation.GetKeyBusyCount());

			generation.InvalidatePublicReads();

			Assert.AreEqual(0L, generation.GetErrorCount());
			Assert.AreEqual(0L, generation.GetTimeoutCount());
			Assert.AreEqual(0L, generation.GetKeyBusyCount());
			Assert.AreEqual(
				1L,
				generation.Collector.Read("N1", config).Opened);
		}

		[TestMethod]
		public void MetricsFirstNodeWriteCannotRaceDepartureOwnership()
		{
			Node node = client.Cluster.Nodes[0];
			MetricsStore session = new(
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
			MetricsStore session = new(
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
			MetricsStore first = new(
				CreateInternalPolicy(),
				new TestMetricsClock(),
				new TestMetricsRandom(0));
			CommandMetricsHandle handle = first.BeginCommand(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true);
			handle.CompleteSuccess(node);
			MetricsStoreSnapshot view = first.Read();
			MetricsNamespaceView ns = GetNamespace(view, node);

			AssertThrows<NotSupportedException>(() =>
				((IDictionary<string, MetricsNodeView>)view.Nodes).Add("x", view.Nodes[node.Name]));
			AssertThrows<NotSupportedException>(() =>
				((IList<long>)ns.Latency[Latency.LatencyType.READ])[0] = 99);

			MetricsStore second = new(
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
			MetricsStore session = new(policy, clock, new TestMetricsRandom());
			ConnectionMetricsHandle handle =
				new(session, session.StartConnection());

			handle.Complete(node, null);

			MetricsStoreSnapshot view = session.Read();
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
			MetricsStore session = new(
				policy,
				new TestMetricsClock(),
				new TestMetricsRandom());

			session.AddConnectionOpened(node);
			session.AddConnectionClosed(node);

			MetricsNodeView nodeView = session.Read().Nodes[node.Name];
			Assert.AreEqual(1L, nodeView.Opened);
			Assert.AreEqual(1L, nodeView.Closed);
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

				MetricsStoreSnapshot view = client.Cluster.ReadMetrics();
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

				MetricsStoreSnapshot view = client.Cluster.ReadMetrics();
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

				MetricsStoreSnapshot view = asyncClient.Cluster.ReadMetrics();
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

		[TestMethod]
		public void ClusterStatsRemainAlwaysOnAcrossMetricsLifecycle()
		{
			client.DisableMetrics();
			Cluster cluster = client.Cluster;
			Node node = cluster.Nodes[0];
			long errors = node.GetErrorCount();
			long timeouts = node.GetTimeoutCount();
			long keyBusy = node.GetKeyBusyCount();

			RecordCompatibilityFailure(
				cluster,
				node,
				new AerospikeException(ResultCode.PARAMETER_ERROR),
				false);

			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				Sampler = MetricsSampler.FromProbability(0)
			});
			try
			{
				RecordCompatibilityFailure(
					cluster,
					node,
					new AerospikeException(ResultCode.KEY_BUSY),
					false);
			}
			finally
			{
				client.DisableMetrics();
			}

			RecordCompatibilityFailure(
				cluster,
				node,
				new AerospikeException(ResultCode.TIMEOUT),
				true);

			NodeStats stats = client.GetClusterStats().nodes.Single(
				stat => ReferenceEquals(stat.node, node));
			Assert.AreEqual(errors + 2, stats.ErrorCount);
			Assert.AreEqual(timeouts + 1, stats.TimeoutCount);
			Assert.AreEqual(keyBusy + 1, stats.KeyBusyCount);
		}

		[TestMethod]
		public void MetricsLifecycleRejectsStaleCompletionsAfterReenable()
		{
			client.DisableMetrics();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true,
				Sampler = MetricsSampler.FromProbability(1)
			};
			client.EnableMetrics(policy);
			Cluster cluster = client.Cluster;
			Node node = cluster.Nodes[0];
			long errors = node.GetErrorCount();
			ApiMetricsCall call = cluster.BeginPointApiMetrics(
				ApiMetricsOperation.Read,
				SuiteHelpers.ns,
				ApiExecutionMode.Blocking);
			MetricsCallContext context = cluster.CaptureApiMetricsContext();
			MetricsRpcAttempt success = cluster.BeginMetricsCommand(
				context,
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true).BeginAttempt();
			MetricsRpcAttempt failure = cluster.BeginMetricsCommand(
				context,
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				false).BeginAttempt();
			ConnectionMetricsHandle connection =
				cluster.StartConnectionMetrics(node);

			client.DisableMetrics();
			client.EnableMetrics(policy);

			success.AddBytes(41, 43);
			Assert.IsTrue(success.TrySucceed(node));
			Assert.IsTrue(failure.TryFail(
				node,
				new AerospikeException(ResultCode.PARAMETER_ERROR),
				false));
			connection.Complete(node, SuiteHelpers.ns);
			call.Dispose();

			MetricsSnapshot snapshot = client.GetMetricsSnapshot();
			NodeMetricsSnapshot current = snapshot.Nodes.Single(
				item => item.Name == node.Name);
			Assert.AreEqual(0L, snapshot.Cluster.CommandCount);
			Assert.AreEqual(0L, current.Connections.Opened);
			Assert.IsFalse(current.Namespaces.Any(
				item => item.Name == SuiteHelpers.ns));
			Assert.AreEqual(0L, snapshot.Cluster.Latency.Read.Sum());
			Assert.AreEqual(errors + 1, node.GetErrorCount());
			client.DisableMetrics();
		}

		[TestMethod]
		public void MetricsNodeLifecycleAggregatesRejoinAndLateGenerationWrites()
		{
			client.DisableMetrics();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true,
				Sampler = MetricsSampler.FromProbability(1)
			};
			client.EnableMetrics(policy);
			Cluster cluster = client.Cluster;
			MetricsStore store = cluster.MetricsStore;
			Node template = cluster.Nodes[0];
			Node first = CreateLifecycleNode(cluster, template, "metrics-rejoin");
			Node second = null;

			try
			{
				store.AdmitNode(first);
				first.EnableMetrics(policy);
				first.CompleteAdmissionMetrics();
				first.GetMetrics().BytesInCounter.Increment("historical", 5);
				first.AddError("counter-only");

				CommandMetricsHandle command = store.BeginCommand(
					"late",
					Latency.LatencyType.READ,
					true);
				MetricsRpcAttempt late = command.BeginAttempt();
				cluster.TransferNodeDeparture(first);
				late.AddBytes(7, 0);
				Assert.IsTrue(late.TrySucceed(first));

				second = CreateLifecycleNode(
					cluster,
					template,
					"metrics-rejoin");
				store.AdmitNode(second);
				second.EnableMetrics(policy);
				second.CompleteAdmissionMetrics();
				second.GetMetrics().BytesInCounter.Increment("current", 11);
				second.AddError("counter-only");

				MetricsNodeView active =
					store.Read().Nodes["metrics-rejoin"];
				Assert.AreEqual(
					5L,
					active.Namespaces["historical"].BytesIn);
				Assert.AreEqual(7L, active.Namespaces["late"].BytesIn);
				Assert.AreEqual(11L, active.Namespaces["current"].BytesIn);

				MetricsNodeView compatibility =
					store.ReadCompatibilityNode(second);
				Assert.AreEqual(
					2L,
					compatibility.Namespaces["counter-only"].Errors);
				Assert.IsTrue(compatibility.Namespaces.ContainsKey("late"));
			}
			finally
			{
				if (second != null && second.Active)
				{
					cluster.TransferNodeDeparture(second);
				}
				client.DisableMetrics();
			}
		}

		private static Node CreateLifecycleNode(
			Cluster cluster,
			Node template,
			string name)
		{
			NodeValidator validator = new()
			{
				name = name,
				primaryHost = template.Host,
				primaryAddress = template.NodeAddress,
				serverVersion = template.serverVersion
			};
			return new Node(cluster, validator);
		}

		private static void RecordCompatibilityFailure(
			Cluster cluster,
			Node node,
			AerospikeException exception,
			bool clientTimeout)
		{
			using ApiMetricsCall call = cluster.BeginPointApiMetrics(
				ApiMetricsOperation.Read,
				SuiteHelpers.ns,
				ApiExecutionMode.Blocking);
			MetricsCallContext context = cluster.CaptureApiMetricsContext();
			MetricsRpcAttempt attempt = cluster.BeginMetricsCommand(
				context,
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				true).BeginAttempt();
			Assert.IsNotNull(attempt);
			Assert.IsTrue(attempt.TryFail(node, exception, clientTimeout));
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

		private static MetricsNamespaceView GetNamespace(MetricsStoreSnapshot view, Node node)
		{
			return view.Nodes[node.Name].Namespaces[SuiteHelpers.ns];
		}

		private static void CompleteFailure(
			MetricsStore session,
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
			MetricsStoreSnapshot view,
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
				MetricsStore session,
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

		private sealed class TestQueryExecutor : IQueryExecutor, IQueryMetrics
		{
			internal Exception Failure { get; init; }
			internal int SuccessCount { get; private set; }
			internal int FailureCount { get; private set; }
			internal int AbandonedCount { get; private set; }

			public bool StopThreads(Exception cause) => true;

			public void CheckForException()
			{
				if (Failure != null)
				{
					throw Failure;
				}
			}

			void IQueryMetrics.RecordQueued(int delta)
			{
			}

			void IQueryMetrics.RecordAbandoned()
			{
				AbandonedCount++;
			}

			void IQueryMetrics.CompleteSuccess()
			{
				SuccessCount++;
			}

			void IQueryMetrics.CompleteFailure()
			{
				FailureCount++;
			}
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
