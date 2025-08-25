/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	public sealed class BatchExecutor
	{
		public static void Execute(Cluster cluster, BatchPolicy policy, IBatchCommand[] commands, BatchStatus status)
		{
			cluster.AddCommandCount();

			if (policy.maxConcurrentThreads == 1 || commands.Length <= 1)
			{
				// Run batch requests sequentially in same thread.
				foreach (IBatchCommand command in commands)
				{
					try
					{
						command.Execute();
					}
					catch (AerospikeException ae)
					{
						if (ae.InDoubt)
						{
							command.SetInDoubt();
						}
						status.SetException(ae);
					}
					catch (Exception e)
					{
						command.SetInDoubt();
						status.SetException(e);
					}
				}
				status.CheckException();
				return;
			}

			// Run batch requests in parallel in separate threads.
			BatchExecutor executor = new BatchExecutor(policy, commands, status);
			executor.Execute();
		}

		public static void Execute(IBatchCommand command, BatchStatus status)
		{
			command.Execute();
			status.CheckException();
		}

		private readonly BatchStatus status;
		private readonly int maxConcurrentThreads;
		private readonly IBatchCommand[] commands;
		private int completedCount;
		private volatile int done;
		private bool completed;

		private BatchExecutor(BatchPolicy policy, IBatchCommand[] commands, BatchStatus status)
		{
			this.commands = commands;
			this.status = status;
			this.maxConcurrentThreads = (policy.maxConcurrentThreads == 0 || policy.maxConcurrentThreads >= commands.Length) ? commands.Length : policy.maxConcurrentThreads;
		}

		internal void Execute()
		{
			// Start threads.
			for (int i = 0; i < maxConcurrentThreads; i++)
			{
				IBatchCommand cmd = commands[i];
				cmd.Parent = this;
				ThreadPool.UnsafeQueueUserWorkItem(cmd.Run, null);
			}

			// Multiple threads write to the batch record array/list, so one might think that memory barriers
			// are needed. That should not be necessary because of this synchronized waitTillComplete().
			WaitTillComplete();

			// Throw an exception if an error occurred.
			status.CheckException();
		}

		internal void OnComplete()
		{
			int finished = Interlocked.Increment(ref completedCount);

			if (finished < commands.Length)
			{
				int nextThread = finished + maxConcurrentThreads - 1;

				// Determine if a new thread needs to be started.
				if (nextThread < commands.Length && done == 0)
				{
					// Start new thread.
					IBatchCommand cmd = commands[nextThread];
					cmd.Parent = this;
					ThreadPool.UnsafeQueueUserWorkItem(cmd.Run, null);
				}
			}
			else
			{
				// Ensure executor succeeds or fails exactly once.
				if (Interlocked.Exchange(ref done, 1) == 0)
				{
					NotifyCompleted();
				}
			}
		}

		internal bool IsDone()
		{
			return done != 0;
		}

		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!completed)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void NotifyCompleted()
		{
			lock (this)
			{
				completed = true;
				Monitor.Pulse(this);
			}
		}
	}
}
