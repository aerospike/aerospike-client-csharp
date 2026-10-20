/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Runtime.ExceptionServices;

namespace Aerospike.Client
{
	public sealed class QueryPartitionExecutor : IQueryExecutor, IQueryMetrics
	{
		private readonly Cluster cluster;
		private readonly QueryPolicy policy;
		private readonly Statement statement;
		private readonly List<QueryThread> threads;
		private readonly CancellationTokenSource cancel;
		private readonly PartitionTracker tracker;
		private readonly RecordSet recordSet;
		private readonly QueryMetricsHandle queryMetrics;
		private readonly ApiMetricsContext apiMetricsContext;
		private volatile Exception exception;
		private int maxConcurrentThreads;
		private int completedCount;
		private int done;
		private int lifecycle;
		private bool threadsComplete;
		private MultiCommand sequentialCommand;

		private const int Running = 0;
		private const int CancelledOrFailed = 1;
		private const int Completed = 2;

		public QueryPartitionExecutor
		(
			Cluster cluster,
			QueryPolicy policy,
			Statement statement,
			int nodeCapacity,
			PartitionTracker tracker
		)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.statement = statement;
			this.threads = new List<QueryThread>(nodeCapacity);
			this.cancel = new CancellationTokenSource();
			this.tracker = tracker;
			this.recordSet = new RecordSet(this, policy.recordQueueSize, cancel.Token);
			this.apiMetricsContext = cluster.CaptureApiMetricsContext();
			this.queryMetrics = cluster.BeginQueryMetrics(apiMetricsContext);
			cluster.AddCommandCount();
			cluster.AddMetricsCommandCount();
			cluster.RecordApiUsage(ApiExecutionMode.Blocking);
			ThreadPool.UnsafeQueueUserWorkItem(this.Run, null);
		}

		public void Run(object obj)
		{
			try
			{
				Execute();
			}
			catch (Exception e)
			{
				StopThreads(e);
			}
		}

		private void Execute()
		{
			ulong taskId = statement.PrepareTaskId();

			while (true)
			{
				List<NodePartitions> list = tracker.AssignPartitionsToNodes(cluster, statement.ns);

				// Initialize maximum number of nodes to query in parallel.
				maxConcurrentThreads = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= list.Count) ? list.Count : policy.maxConcurrentNodes;

				bool parallel = maxConcurrentThreads > 1 && list.Count > 1;

				lock (threads)
				{
					// Cancellation is permanent. Round completion is reset only after
					// cancellation has been checked under the same lock.
					if (Volatile.Read(ref lifecycle) != Running)
					{
						break;
					}

					Volatile.Write(ref done, 0);
					threads.Clear();

					if (parallel)
					{
						foreach (NodePartitions nodePartitions in list)
						{
							MultiCommand command = new QueryPartitionCommand(cluster, policy, statement, taskId, recordSet, tracker, nodePartitions);
							command.SetApiMetricsContext(apiMetricsContext);
							threads.Add(new QueryThread(this, command));
						}

						for (int i = 0; i < maxConcurrentThreads; i++)
						{
							ThreadPool.UnsafeQueueUserWorkItem(threads[i].Run, null);
						}
					}
				}

				if (parallel)
				{
					WaitTillComplete();
				}
				else
				{
					foreach (NodePartitions nodePartitions in list)
					{
						MultiCommand command = new QueryPartitionCommand(cluster, policy, statement, taskId, recordSet, tracker, nodePartitions);
						command.SetApiMetricsContext(apiMetricsContext);

						lock (threads)
						{
							if (Volatile.Read(ref lifecycle) != Running)
							{
								break;
							}
							sequentialCommand = command;
						}

						try
						{
							if (Volatile.Read(ref lifecycle) == Running &&
								command.IsValid())
							{
								command.Execute();
							}
						}
						finally
						{
							lock (threads)
							{
								if (ReferenceEquals(sequentialCommand, command))
								{
									sequentialCommand = null;
								}
							}
						}
					}
				}

				if (Volatile.Read(ref lifecycle) != Running)
				{
					break;
				}

				if (exception != null)
				{
					break;
				}

				if (tracker.IsClusterComplete(cluster, policy))
				{
					// All partitions received.
					if (Interlocked.CompareExchange(
						ref lifecycle,
						Completed,
						Running) == Running)
					{
						recordSet.Put(RecordSet.END);
					}
					break;
				}

				if (policy.sleepBetweenRetries > 0)
				{
					// Sleep before trying again.
					Util.Sleep(policy.sleepBetweenRetries);
				}

				Interlocked.Exchange(ref completedCount, 0);
				threadsComplete = false;
				exception = null;

				// taskId must be reset on next pass to avoid server duplicate query detection.
				taskId = RandomShift.ThreadLocalInstance.NextLong();
			}
		}

		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!threadsComplete)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void NotifyCompleted()
		{
			lock (this)
			{
				threadsComplete = true;
				Monitor.Pulse(this);
			}
		}

		private void ThreadCompleted()
		{
			int finished = Interlocked.Increment(ref completedCount);

			if (finished < threads.Count)
			{
				int nextThread = finished + maxConcurrentThreads - 1;

				// Determine if a new thread needs to be started.
				if (nextThread < threads.Count &&
					done == 0 &&
					Volatile.Read(ref lifecycle) == Running)
				{
					// Start new thread.
					ThreadPool.UnsafeQueueUserWorkItem(threads[nextThread].Run, null);
				}
			}
			else
			{
				// All threads complete.  Tell RecordSet thread to return complete to user
				// if an exception has not already occurred.
				if (Interlocked.Exchange(ref done, 1) == 0)
				{
					NotifyCompleted();
				}
			}
		}

		public bool StopThreads(Exception cause)
		{
			if (Interlocked.CompareExchange(
				ref lifecycle,
				CancelledOrFailed,
				Running) == Running)
			{
				exception = cause;
				Interlocked.Exchange(ref done, 1);

				// Send stop signal to threads.
				// Must synchronize here because this method can be called from the main
				// RecordSet thread (user calls close() before retrieving all records)
				// which may conflict with the parallel QueryPartitionExecutor thread.
				lock (threads)
				{
					foreach (QueryThread thread in threads)
					{
						thread.Stop();
					}
					sequentialCommand?.Stop();
				}
				cancel.Cancel();
				recordSet.Abort();
				NotifyCompleted();
				return true;
			}
			return false;
		}

		public void CheckForException()
		{
			// Throw an exception if an error occurred.
			if (exception != null)
			{
				tracker.PartitionError();

				if (exception is AerospikeException ae)
				{
					// Preserve the server result and extended error detail captured by
					// the query command, along with the worker thread's stack trace.
					ae.Iteration = tracker.iteration;
					ExceptionDispatchInfo.Capture(ae).Throw();
				}

				// Preserve the existing public behavior for unexpected client errors.
				AerospikeException wrapper = new("Query Failed: " + exception.Message, exception)
				{
					Iteration = tracker.iteration
				};
				throw wrapper;
			}
		}

		void IQueryMetrics.RecordQueued(int delta)
		{
			queryMetrics.AddQueued(delta);
		}

		void IQueryMetrics.RecordAbandoned()
		{
			queryMetrics.AddAbandoned();
		}

		public RecordSet RecordSet
		{
			get
			{
				return recordSet;
			}
		}

		private sealed class QueryThread
		{
			private readonly QueryPartitionExecutor parent;
			private readonly MultiCommand command;

			public QueryThread(QueryPartitionExecutor parent, MultiCommand command)
			{
				this.parent = parent;
				this.command = command;
			}

			public void Run(object obj)
			{
				try
				{
					if (command.IsValid())
					{
						command.Execute();
					}
					parent.ThreadCompleted();
				}
				catch (Exception e)
				{
					// Terminate other query threads.
					parent.StopThreads(e);
				}
			}

			/// <summary>
			/// Send stop signal to each thread.
			/// </summary>
			public void Stop()
			{
				command.Stop();
			}
		}
	}
}
