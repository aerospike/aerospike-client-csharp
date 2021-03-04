/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.Net.Sockets;
using System.Threading;

namespace Aerospike.Client
{
	internal sealed class AsyncCommandDelayingQueue : AsyncCommandQueueBase
	{
		private readonly ConcurrentQueue<SocketAsyncEventArgs> argsQueue = new ConcurrentQueue<SocketAsyncEventArgs>();
		private readonly ConcurrentQueue<AsyncCommand> delayQueue = new ConcurrentQueue<AsyncCommand>();
		private readonly WaitCallback schedulingJobCallback;
		private readonly int maxCommandsInQueue;
		private volatile int delayQueueCount;
		private volatile int jobScheduled;

		public AsyncCommandDelayingQueue(AsyncClientPolicy policy)
		{
			schedulingJobCallback = new WaitCallback(ExclusiveScheduleCommands);
			maxCommandsInQueue = policy.asyncMaxCommandsInQueue;
		}

		// Releases a SocketEventArgs object to the pool.
		public override void ReleaseArgs(SocketAsyncEventArgs e)
		{
			AsyncCommand command;

			if (delayQueue.TryDequeue(out command))
			{
				if (maxCommandsInQueue > 0)
				{
					Interlocked.Decrement(ref delayQueueCount);
				}
				command.ExecuteAsync(e);
			}
			else
			{
				argsQueue.Enqueue(e);
				TriggerCommandScheduling();
			}
		}

		// Schedules a command for later execution.
		public override void ScheduleCommand(AsyncCommand command)
		{
			SocketAsyncEventArgs e;

			// Try to dequeue one SocketAsyncEventArgs object from the queue and execute it.
			if (argsQueue.TryDequeue(out e))
			{
				// If there are no awaiting command, the current command can be executed immediately.
				if (delayQueue.IsEmpty)
				{
					command.ExecuteInline(e);
					return;
				}
				else
				{
					argsQueue.Enqueue(e);
				}
			}
			else
			{
				// Queue command for later execution. Delay queue count increment precedes Enqueue and
				// delay queue count decrement succeeds Dequeue. This might cause maximum allowed delay 
				// count to be slightly less than maxCommandsInQueue (due to race conditions), but never
				// greater than maxCommandsInQueue.
				//
				// An alternate solution would be to use BlockingCollection to wrap the queue and perform
				// bounds checking. This would guarantee strict maxCommandsInQueue adherence, but result 
				// in reduced performance.
				//
				// Allowing slighty less than maxCommandsInQueue is not a deal breaker because standard
				// maxCommandsInQueue values should be at least 1000 when used.  Thus, the more 
				// performant solution was chosen.
				if (maxCommandsInQueue == 0 || Interlocked.Increment(ref delayQueueCount) <= maxCommandsInQueue)
				{
					delayQueue.Enqueue(command);
				}
				else
				{
					Interlocked.Decrement(ref delayQueueCount);
					throw new AerospikeException.CommandRejected();
				}
			}

			TriggerCommandScheduling();
		}

		// Schedule exactly once the job that will execute queued commands.
		private void TriggerCommandScheduling()
		{
			if (Interlocked.CompareExchange(ref jobScheduled, 1, 0) == 0)
			{
				ThreadPool.UnsafeQueueUserWorkItem(schedulingJobCallback, null);
			}
		}

		// Schedule as many commands as possible.
		private void ExclusiveScheduleCommands(object state)
		{
			do
			{
				bool lockTaken = false;
				try
				{
					// Lock on delayQueue for exclusive execution of the job.
					Monitor.TryEnter(delayQueue, ref lockTaken); // If we can't enter the lock, it means another instance of the job is already doing the work.
					if (!lockTaken) return;

					jobScheduled = 1; // Volatile Write. At this point, the job cannot be rescheduled.

					// Try scheduling as many commands as possible.
					SocketAsyncEventArgs e;
					while (!delayQueue.IsEmpty && argsQueue.TryDequeue(out e))
					{
						AsyncCommand dequeuedCommand;
						if (delayQueue.TryDequeue(out dequeuedCommand))
						{
							if (maxCommandsInQueue > 0)
							{
								Interlocked.Decrement(ref delayQueueCount);
							}
							dequeuedCommand.ExecuteAsync(e);
						}
						else
						{
							argsQueue.Enqueue(e);
						}
					}
				}
				finally
				{
					if (lockTaken)
					{
						jobScheduled = 0; // Volatile Write. At this point, the job can be rescheduled.
						Monitor.Exit(delayQueue);
					}
				}
			}
			while (!(delayQueue.IsEmpty || argsQueue.IsEmpty)); // Re-execute the job as long as both queues are non-empty
		}
	}
}
