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
using System.Net;
using System.Net.Sockets;
using System.Reflection;

namespace Aerospike.Test
{
	/// <summary>
	/// Drives the real <see cref="AsyncCommand"/> state machine (CheckTimeout,
	/// Finish, OnError, OnSocketError, AlreadyCompleted and retry cloning) with
	/// controlled connections and callbacks. Timeouts are made due by installing
	/// active stopwatches against zero timeouts, so no test waits on wall-clock
	/// time. Metrics go to a private store per command so deltas are exact.
	/// </summary>
	[TestClass]
	public class TestAsyncMetricsRaces : TestAsync
	{
		private const int RaceIterations = 100;
		private const int SentBytes = 10;
		private const int ReceivedBytes = 20;

		[TestMethod]
		public void MetricsAsyncTotalTimeoutThenLateFinishCommitsTimeoutOnly()
		{
			using AsyncRaceHarness harness = new();
			AsyncRun run = harness.Start(0);
			run.Connection.DeliverResponse(ReceivedBytes);

			run.Command.ExpireTotalTimeout();
			Assert.IsFalse(run.Command.CheckTimeout());
			Assert.AreEqual(1, run.Outcome.Failures);
			run.Command.Succeed();

			Assert.AreEqual(0, run.Outcome.Successes);
			Assert.AreEqual(1, run.Outcome.Failures);
			Assert.IsInstanceOfType(
				run.Outcome.LastFailure,
				typeof(AerospikeException.Timeout));
			AttemptTotals totals = run.Read();
			Assert.AreEqual(1L, totals.Timeouts);
			Assert.AreEqual(1L, totals.ClientTimeouts);
			Assert.AreEqual(1L, totals.StoreClientTimeouts);
			Assert.AreEqual(1L, totals.CompatibilityTimeouts);
			Assert.AreEqual(0L, totals.Latency);
			Assert.AreEqual(0L, totals.BytesIn);
			Assert.AreEqual(0L, totals.BytesOut);
			Assert.AreEqual(1, run.Connection.CloseCount);
			Assert.AreEqual(0, run.Connection.ResetCount);
		}

		[TestMethod]
		public void MetricsAsyncFinishThenTotalTimeoutCommitsSuccessOnly()
		{
			using AsyncRaceHarness harness = new();
			AsyncRun run = harness.Start(0);
			run.Connection.DeliverResponse(ReceivedBytes);

			run.Command.Succeed();
			run.Command.ExpireTotalTimeout();
			Assert.IsFalse(run.Command.CheckTimeout());

			Assert.AreEqual(1, run.Outcome.Successes);
			Assert.AreEqual(0, run.Outcome.Failures);
			AttemptTotals totals = run.Read();
			Assert.AreEqual(1L, totals.Latency);
			Assert.AreEqual(ReceivedBytes, totals.BytesIn);
			Assert.AreEqual(SentBytes, totals.BytesOut);
			Assert.AreEqual(0L, totals.Timeouts);
			Assert.AreEqual(0L, totals.CompatibilityTimeouts);
			Assert.AreEqual(0, run.Connection.CloseCount);
			Assert.AreEqual(1, run.Connection.ResetCount);
		}

		[TestMethod]
		public void MetricsAsyncTotalTimeoutRacingFinishCommitsOneTerminal()
		{
			using AsyncRaceHarness harness = new();

			for (int i = 0; i < RaceIterations; i++)
			{
				AsyncRun run = harness.Start(0);
				run.Connection.DeliverResponse(ReceivedBytes);
				run.Command.ExpireTotalTimeout();

				RunConcurrently(
					() => run.Command.CheckTimeout(),
					run.Command.Succeed);

				Assert.AreEqual(
					1,
					run.Outcome.Successes + run.Outcome.Failures,
					$"Iteration {i} notified the user more than once.");
				AttemptTotals totals = run.Read();

				if (run.Outcome.Successes == 1)
				{
					Assert.AreEqual(1L, totals.Latency);
					Assert.AreEqual(ReceivedBytes, totals.BytesIn);
					Assert.AreEqual(SentBytes, totals.BytesOut);
					Assert.AreEqual(0L, totals.Timeouts);
					Assert.AreEqual(0L, totals.CompatibilityTimeouts);
				}
				else
				{
					Assert.AreEqual(0L, totals.Latency);
					Assert.AreEqual(0L, totals.BytesIn);
					Assert.AreEqual(0L, totals.BytesOut);
					Assert.AreEqual(1L, totals.Timeouts);
					Assert.AreEqual(1L, totals.ClientTimeouts);
					Assert.AreEqual(1L, totals.CompatibilityTimeouts);
				}
				Assert.AreEqual(
					1,
					run.Connection.CloseCount + run.Connection.ResetCount);
			}
		}

		[TestMethod]
		public void MetricsAsyncSocketTimeoutOwnsLateFinish()
		{
			using AsyncRaceHarness harness = new();
			AsyncRun run = harness.Start(0);
			run.Connection.DeliverResponse(ReceivedBytes);

			run.Command.ExpireSocketTimeout();
			Assert.IsFalse(run.Command.CheckTimeout());
			Assert.AreEqual(0, run.Outcome.Successes + run.Outcome.Failures);
			run.Command.Succeed();

			AssertSocketTimeoutFailure(run);
		}

		[TestMethod]
		public void MetricsAsyncSocketTimeoutOwnsLateSocketCallback()
		{
			using AsyncRaceHarness harness = new();
			AsyncRun run = harness.Start(0);
			run.Connection.DeliverResponse(ReceivedBytes);

			run.Command.ExpireSocketTimeout();
			Assert.IsFalse(run.Command.CheckTimeout());
			run.Command.OnSocketError(SocketError.OperationAborted);

			AssertSocketTimeoutFailure(run);
		}

		[TestMethod]
		public void MetricsAsyncTotalTimeoutRacingOnErrorCommitsOneFailure()
		{
			using AsyncRaceHarness harness = new();

			for (int i = 0; i < RaceIterations; i++)
			{
				AsyncRun run = harness.Start(0);
				run.Command.ExpireTotalTimeout();

				RunConcurrently(
					() => run.Command.CheckTimeout(),
					() => run.Command.OnError(
						new AerospikeException(ResultCode.PARAMETER_ERROR)));

				AssertOneFailure(run, i);
			}
		}

		[TestMethod]
		public void MetricsAsyncSocketTimeoutRacingOnSocketErrorCommitsOneFailure()
		{
			using AsyncRaceHarness harness = new();

			for (int i = 0; i < RaceIterations; i++)
			{
				AsyncRun run = harness.Start(0);
				run.Command.ExpireSocketTimeout();

				RunConcurrently(
					() => run.Command.CheckTimeout(),
					() => run.Command.OnSocketError(SocketError.ConnectionReset));

				AssertOneFailure(run, i);
			}
		}

		[TestMethod]
		public void MetricsAsyncSocketTimeoutRetrySucceedsOnClone()
		{
			using AsyncRaceHarness harness = new();
			AsyncRun run = harness.Start(1, allowClone: true);
			ControlledAsyncConnection retryConnection = harness.PoolConnection();

			run.Command.ExpireSocketTimeout();
			Assert.IsFalse(run.Command.CheckTimeout());
			Assert.AreEqual(1, run.Connection.CloseCount);
			// The closed socket's pending receive fails; the timeout owns it and
			// retries on a pooled connection.
			run.Command.OnSocketError(SocketError.OperationAborted);

			Assert.AreEqual(1, run.Outcome.Clones.Count);
			TestAsyncMetricsCommand clone = run.Outcome.Clones[0];
			Assert.IsTrue(retryConnection.Sent.IsSet);
			Assert.AreSame(clone, retryConnection.Command);
			Assert.AreEqual(0, run.Outcome.Successes + run.Outcome.Failures);

			retryConnection.DeliverResponse(ReceivedBytes);
			clone.Succeed();

			Assert.AreEqual(1, run.Outcome.Successes);
			Assert.AreEqual(0, run.Outcome.Failures);
			AttemptTotals totals = run.Read();
			Assert.AreEqual(1L, totals.Retries);
			Assert.AreEqual(1L, totals.Timeouts);
			Assert.AreEqual(1L, totals.ClientTimeouts);
			Assert.AreEqual(1L, totals.CompatibilityTimeouts);
			Assert.AreEqual(0L, totals.CompatibilityErrors);
			Assert.AreEqual(1L, totals.Latency);
			Assert.AreEqual(ReceivedBytes, totals.BytesIn);
			Assert.AreEqual(SentBytes, totals.BytesOut);
			// Once when pooled for the retry, once when the clone returned it.
			Assert.AreEqual(2, retryConnection.ResetCount);
			Assert.AreEqual(0, retryConnection.CloseCount);
		}

		private static void AssertSocketTimeoutFailure(AsyncRun run)
		{
			Assert.AreEqual(0, run.Outcome.Successes);
			Assert.AreEqual(1, run.Outcome.Failures);
			Assert.IsInstanceOfType(
				run.Outcome.LastFailure,
				typeof(AerospikeException.Timeout));
			Assert.AreEqual(0, run.Outcome.Clones.Count);
			AttemptTotals totals = run.Read();
			Assert.AreEqual(1L, totals.Timeouts);
			Assert.AreEqual(1L, totals.ClientTimeouts);
			Assert.AreEqual(1L, totals.CompatibilityTimeouts);
			Assert.AreEqual(0L, totals.CompatibilityErrors);
			Assert.AreEqual(0L, totals.Errors);
			Assert.AreEqual(0L, totals.Latency);
			Assert.AreEqual(0L, totals.BytesIn);
			Assert.AreEqual(0L, totals.BytesOut);
			Assert.AreEqual(0L, totals.Retries);
			Assert.AreEqual(1, run.Connection.CloseCount);
		}

		private static void AssertOneFailure(AsyncRun run, int iteration)
		{
			Assert.AreEqual(
				0,
				run.Outcome.Successes,
				$"Iteration {iteration} reported success.");
			Assert.AreEqual(
				1,
				run.Outcome.Failures,
				$"Iteration {iteration} notified failure more than once.");
			AttemptTotals totals = run.Read();
			Assert.AreEqual(
				1L,
				totals.Errors + totals.Timeouts,
				$"Iteration {iteration} sampled failures.");
			Assert.AreEqual(
				1L,
				totals.CompatibilityErrors + totals.CompatibilityTimeouts,
				$"Iteration {iteration} compatibility failures.");
			Assert.AreEqual(totals.Timeouts, totals.ClientTimeouts);
			Assert.AreEqual(0L, totals.Latency);
			Assert.AreEqual(0L, totals.Retries);
			Assert.IsTrue(
				run.Connection.CloseCount + run.Connection.ResetCount <= 1,
				$"Iteration {iteration} released its connection twice.");
		}

		/// <summary>
		/// Release all actions together on dedicated threads. Parallel.Invoke may
		/// inline actions on one thread, which would deadlock the barrier.
		/// </summary>
		private static void RunConcurrently(params Action[] actions)
		{
			using Barrier barrier = new(actions.Length);
			Exception[] errors = new Exception[actions.Length];
			Thread[] threads = new Thread[actions.Length];

			for (int i = 0; i < actions.Length; i++)
			{
				int index = i;
				threads[i] = new Thread(() =>
				{
					try
					{
						barrier.SignalAndWait();
						actions[index]();
					}
					catch (Exception e)
					{
						errors[index] = e;
					}
				})
				{
					IsBackground = true
				};
			}

			foreach (Thread thread in threads)
			{
				thread.Start();
			}
			foreach (Thread thread in threads)
			{
				Assert.IsTrue(thread.Join(TimeSpan.FromSeconds(30)));
			}
			foreach (Exception error in errors)
			{
				if (error != null)
				{
					throw new AssertFailedException("Concurrent action failed.", error);
				}
			}
		}

		private sealed record AttemptTotals(
			long Latency,
			long Timeouts,
			long ClientTimeouts,
			long Errors,
			long BytesIn,
			long BytesOut,
			long Retries,
			long StoreClientTimeouts,
			long CompatibilityTimeouts,
			long CompatibilityErrors);

		private sealed class AsyncRun
		{
			internal readonly MetricsStore Store;
			internal readonly AsyncNode Node;
			internal readonly TestAsyncMetricsCommand Command;
			internal readonly ControlledAsyncConnection Connection;
			internal readonly AsyncOutcome Outcome;

			internal AsyncRun(
				MetricsStore store,
				AsyncNode node,
				TestAsyncMetricsCommand command,
				ControlledAsyncConnection connection,
				AsyncOutcome outcome)
			{
				Store = store;
				Node = node;
				Command = command;
				Connection = connection;
				Outcome = outcome;
			}

			internal AttemptTotals Read()
			{
				MetricsStoreSnapshot view = Store.Read();
				MetricsNamespaceView ns = null;

				if (view.Nodes.TryGetValue(Node.Name, out MetricsNodeView nodeView))
				{
					nodeView.Namespaces.TryGetValue(SuiteHelpers.ns, out ns);
				}

				MetricsNodeGeneration generation = Store.RegisterNode(Node);
				return new AttemptTotals(
					ns?.Latency[Latency.LatencyType.READ].Sum() ?? 0,
					ns?.Timeouts ?? 0,
					ns?.ClientTimeouts ?? 0,
					ns?.Errors ?? 0,
					ns?.BytesIn ?? 0,
					ns?.BytesOut ?? 0,
					view.CommandRetries,
					view.ClientTimeouts,
					generation.GetTimeoutCount(SuiteHelpers.ns),
					generation.GetErrorCount(SuiteHelpers.ns));
			}
		}

		/// <summary>
		/// Callback and clone record shared by a command and its retries.
		/// </summary>
		private sealed class AsyncOutcome
		{
			private readonly List<TestAsyncMetricsCommand> clones = new();
			private int successes;
			private int failures;
			private volatile AerospikeException lastFailure;

			internal bool AllowClone { get; init; }
			internal int Successes => Volatile.Read(ref successes);
			internal int Failures => Volatile.Read(ref failures);
			internal AerospikeException LastFailure => lastFailure;

			internal IReadOnlyList<TestAsyncMetricsCommand> Clones
			{
				get
				{
					lock (clones)
					{
						return clones.ToArray();
					}
				}
			}

			internal void Success()
			{
				Interlocked.Increment(ref successes);
			}

			internal void Failure(AerospikeException ae)
			{
				lastFailure = ae;
				Interlocked.Increment(ref failures);
			}

			internal void AddClone(TestAsyncMetricsCommand clone)
			{
				lock (clones)
				{
					clones.Add(clone);
				}
			}
		}

		/// <summary>
		/// Dedicated node and loopback peer so controlled connections never enter
		/// a live node's pool.
		/// </summary>
		private sealed class AsyncRaceHarness : IDisposable
		{
			private static readonly FieldInfo ClusterField =
				typeof(AsyncClient).GetField(
					"cluster",
					BindingFlags.Instance | BindingFlags.NonPublic);

			private readonly AsyncCluster cluster;
			private readonly TcpListener peer;
			private readonly List<Socket> accepted = new();
			private readonly AsyncNode node;

			internal AsyncRaceHarness()
			{
				cluster = (AsyncCluster)ClusterField.GetValue(SuiteHelpers.asyncClient);
				peer = new TcpListener(IPAddress.Loopback, 0);
				peer.Start();
				int port = ((IPEndPoint)peer.LocalEndpoint).Port;
				node = new AsyncNode(
					cluster,
					new NodeValidator
					{
						name = "metrics-async-race",
						primaryHost = new Host("127.0.0.1", port),
						primaryAddress = new IPEndPoint(IPAddress.Loopback, port),
						serverVersion = cluster.Nodes[0].serverVersion
					});
			}

			internal AsyncRun Start(int maxRetries, bool allowClone = false)
			{
				DrainPool();
				node.ResetErrorRate();

				MetricsStore store = new(new MetricsPolicy
				{
					Exporter = MetricsExporterMode.None,
					OperationalEnabled = true,
					Sampler = MetricsSampler.FromProbability(1)
				});
				store.AdmitNode(node);
				AsyncOutcome outcome = new() { AllowClone = allowClone };
				TestAsyncMetricsCommand command = new(
					cluster,
					new Policy
					{
						socketTimeout = 0,
						totalTimeout = 0,
						maxRetries = maxRetries
					},
					node,
					store.BeginCommand(
						SuiteHelpers.ns,
						Latency.LatencyType.READ,
						true),
					outcome);
				ControlledAsyncConnection connection = PoolConnection();

				command.Start();

				Assert.IsTrue(connection.Sent.IsSet);
				Assert.AreSame(command, connection.Command);
				Assert.AreEqual(1, connection.SendCount);
				connection.ClearCounts();
				return new AsyncRun(store, node, command, connection, outcome);
			}

			internal ControlledAsyncConnection PoolConnection()
			{
				ControlledAsyncConnection connection = new(node, Connect);
				node.PutAsyncConnection(connection);
				return connection;
			}

			private void Connect(Socket socket)
			{
				socket.Connect((IPEndPoint)peer.LocalEndpoint);
				Socket server = peer.AcceptSocket();

				lock (accepted)
				{
					accepted.Add(server);
				}
			}

			private void DrainPool()
			{
				AsyncConnection pooled;

				while ((pooled = node.GetAsyncConnection()) != null)
				{
					pooled.Close();
				}
			}

			public void Dispose()
			{
				DrainPool();
				node.Close();
				peer.Stop();

				lock (accepted)
				{
					foreach (Socket socket in accepted)
					{
						socket.Dispose();
					}
					accepted.Clear();
				}
			}
		}

		/// <summary>
		/// Real connected socket whose send/receive completions are driven by the
		/// test. Reset and close are counted so connection release is observable.
		/// </summary>
		private sealed class ControlledAsyncConnection : AsyncConnection
		{
			private int sendCount;
			private int receiveCount;
			private int resetCount;
			private int closeCount;

			internal ControlledAsyncConnection(AsyncNode node, Action<Socket> connect)
				: base(node, null)
			{
				connect(socket);
			}

			internal ManualResetEventSlim Sent { get; } = new(false);
			internal int SendCount => Volatile.Read(ref sendCount);
			internal int ReceiveCount => Volatile.Read(ref receiveCount);
			internal int ResetCount => Volatile.Read(ref resetCount);
			internal int CloseCount => Volatile.Read(ref closeCount);

			internal void ClearCounts()
			{
				Volatile.Write(ref resetCount, 0);
				Volatile.Write(ref closeCount, 0);
			}

			internal void DeliverResponse(int bytes)
			{
				((IAsyncByteProgress)command).OnBytesReceived(bytes);
			}

			public override void Connect(IPEndPoint address)
			{
				throw new InvalidOperationException(
					"Controlled connections are pooled and never opened by the command.");
			}

			public override void Send(byte[] buffer, int offset, int count)
			{
				((IAsyncByteProgress)command).OnBytesSent(SentBytes);
				Interlocked.Increment(ref sendCount);
				Sent.Set();
			}

			public override void Receive(byte[] buffer, int offset, int count)
			{
				Interlocked.Increment(ref receiveCount);
			}

			public override void Reset()
			{
				Interlocked.Increment(ref resetCount);
			}

			public override void Close()
			{
				Interlocked.Increment(ref closeCount);
				base.Close();
			}
		}

		private sealed class TestAsyncMetricsCommand : AsyncCommand
		{
			private static readonly FieldInfo CommandMetricsField =
				typeof(AsyncCommand).GetField(
					"commandMetrics",
					BindingFlags.Instance | BindingFlags.NonPublic);
			private static readonly FieldInfo CommandMetricsStartedField =
				typeof(AsyncCommand).GetField(
					"commandMetricsStarted",
					BindingFlags.Instance | BindingFlags.NonPublic);
			private static readonly FieldInfo TotalWatchField =
				typeof(AsyncCommand).GetField(
					"totalWatch",
					BindingFlags.Instance | BindingFlags.NonPublic);
			private static readonly FieldInfo SocketWatchField =
				typeof(AsyncCommand).GetField(
					"socketWatch",
					BindingFlags.Instance | BindingFlags.NonPublic);
			private static readonly MethodInfo ExecuteCommandMethod =
				typeof(AsyncCommand).GetMethod(
					"ExecuteCommand",
					BindingFlags.Instance | BindingFlags.NonPublic);

			private readonly AsyncNode target;
			private readonly AsyncOutcome outcome;

			internal TestAsyncMetricsCommand(
				AsyncCluster cluster,
				Policy policy,
				AsyncNode target,
				CommandMetricsHandle metrics,
				AsyncOutcome outcome)
				: base(cluster, policy, SuiteHelpers.ns)
			{
				this.target = target;
				this.outcome = outcome;
				CommandMetricsField.SetValue(this, metrics);
				CommandMetricsStartedField.SetValue(this, true);
			}

			private TestAsyncMetricsCommand(TestAsyncMetricsCommand other)
				: base(other)
			{
				target = other.target;
				outcome = other.outcome;
			}

			/// <summary>
			/// Run the production attempt start: node selection, attempt metrics,
			/// pooled connection checkout and request send.
			/// </summary>
			internal void Start()
			{
				ExecuteCommandMethod.Invoke(this, null);
			}

			// With zero timeouts, any active stopwatch is already past due.
			internal void ExpireTotalTimeout()
			{
				TotalWatchField.SetValue(this, ValueStopwatch.StartNew());
			}

			internal void ExpireSocketTimeout()
			{
				SocketWatchField.SetValue(this, ValueStopwatch.StartNew());
			}

			internal void Succeed()
			{
				Finish();
			}

			protected internal override Node GetNode(Cluster cluster) => target;

			protected override Latency.LatencyType GetLatencyType() =>
				Latency.LatencyType.READ;

			protected internal override void WriteBuffer()
			{
			}

			protected internal override AsyncCommand CloneCommand()
			{
				if (!outcome.AllowClone)
				{
					return null;
				}

				TestAsyncMetricsCommand clone = new(this);
				outcome.AddClone(clone);
				return clone;
			}

			protected internal override void ParseCommand()
			{
			}

			protected internal override bool PrepareRetry(bool timeout) => true;

			protected internal override void OnSuccess()
			{
				outcome.Success();
			}

			protected internal override void OnFailure(AerospikeException ae)
			{
				outcome.Failure(ae);
			}
		}
	}
}
