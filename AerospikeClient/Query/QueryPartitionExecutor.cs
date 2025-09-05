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
namespace Aerospike.Client
{
	public sealed class QueryPartitionExecutor : IQueryExecutor
	{
		private readonly Cluster cluster;
		private readonly QueryPolicy policy;
		private readonly Statement statement;
		private readonly List<QueryThread> threads;
		private readonly CancellationTokenSource cancel;
		private readonly PartitionTracker tracker;
		private readonly RecordSet recordSet;
		private volatile Exception exception;
		private int maxConcurrentThreads;
		private int completedCount;
		private int done;
		private bool threadsComplete;

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
			cluster.AddCommandCount();
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
					// RecordSet thread may have aborted query, so check done under lock.
					if (done == 1)
					{
						break;
					}

					threads.Clear();

					if (parallel)
					{
						foreach (NodePartitions nodePartitions in list)
						{
							MultiCommand command = new QueryPartitionCommand(cluster, policy, statement, taskId, recordSet, tracker, nodePartitions);
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
						command.Execute();
					}
				}

				if (exception != null)
				{
					break;
				}

				// Set done to false so RecordSet thread has chance to close early again.
				Interlocked.Exchange(ref done, 0);

				if (tracker.IsClusterComplete(cluster, policy))
				{
					// All partitions received.
					recordSet.Put(RecordSet.END);
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
				if (nextThread < threads.Count && done == 0)
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
			// There is no need to stop threads if all threads have already completed.
			if (Interlocked.Exchange(ref done, 1) == 0)
			{
				exception = cause;

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
				// Wrap exception because throwing will reset the exception's stack trace.
				// Wrapped exceptions preserve the stack trace in the inner exception.
				AerospikeException ae = new AerospikeException("Query Failed: " + exception.Message, exception);
				tracker.PartitionError();
				ae.Iteration = tracker.iteration;
				throw ae;
			}
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
