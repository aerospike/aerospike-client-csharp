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
using System;
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
		private int delayQueueCount;
		private volatile int jobScheduled;

		public AsyncCommandDelayingQueue(AsyncClientPolicy policy)
		{
			schedulingJobCallback = new WaitCallback(ProcessDelayQueueExclusive);
			maxCommandsInQueue = policy.asyncMaxCommandsInQueue;
		}

		public override void ReleaseArgs(SocketAsyncEventArgs args)
		{
			// Release command slot.
			AsyncCommand command;

			// Check delay queue.
			if (delayQueue.TryDequeue(out command))
			{
				// Use command slot to execute delayed command.
				if (maxCommandsInQueue > 0)
				{
					Interlocked.Decrement(ref delayQueueCount);
				}

				try
				{
					command.ExecuteAsync(args);
				}
				catch (Exception e)
				{
					CommandFailed(command, args, e);
				}
				finally
				{
					if (!argsQueue.IsEmpty)
					{
						ProcessDelayQueue();
					}
				}
			}
			else
			{
				// Put command slot back into pool.
				// Do not process delay queue.
				argsQueue.Enqueue(args);
			}
		}

		public override void ScheduleCommand(AsyncCommand command)
		{
			// Schedule command for execution.
			SocketAsyncEventArgs args;

			// Check if command slot is available.
			if (argsQueue.TryDequeue(out args))
			{
				// Command slot is available. Check delay queue.
				AsyncCommand delayedCommand;

				if (delayQueue.TryDequeue(out delayedCommand))
				{
					// Commands in delay queue take precedence over new commands.
					// Put new command in delay queue and use command slot to execute 
					// delayedCommand. delayQueueCount remains the same. 
					delayQueue.Enqueue(command);

					try
					{
						delayedCommand.ExecuteAsync(args);
					}
					catch (Exception e)
					{
						CommandFailed(delayedCommand, args, e);
					}
					finally
					{
						ProcessDelayQueue();
					}
				}
				else
				{
					// There are no commands in the delay queue to process.
					// Use command slot to execute new command immediately.
					command.ExecuteInline(args);
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
					// Add command to delay queue. There are no available command slots, so do not
					// process delay queue further.
					delayQueue.Enqueue(command);
				}
				else
				{
					Interlocked.Decrement(ref delayQueueCount);
					throw new AerospikeException.CommandRejected();
				}
			}
		}

		private void ProcessDelayQueue()
		{
			// Schedule exactly once the job that will execute delayed commands.
			if (Interlocked.CompareExchange(ref jobScheduled, 1, 0) == 0)
			{
				ThreadPool.UnsafeQueueUserWorkItem(schedulingJobCallback, null);
			}
		}

		private void ProcessDelayQueueExclusive(object state)
		{
			// Schedule as many commands as possible.
			bool lockTaken = false;

			try
			{
				// Lock on delayQueue for exclusive execution of the job.
				Monitor.TryEnter(delayQueue, ref lockTaken);

				// If we can't enter the lock, it means another instance of the job is already doing the work.
				if (!lockTaken)
				{
					return;
				}

				// Volatile Write. At this point, the job cannot be rescheduled.
				jobScheduled = 1; 

				// Try scheduling as many commands as possible.
				SocketAsyncEventArgs args;

				while (!delayQueue.IsEmpty && argsQueue.TryDequeue(out args))
				{
					AsyncCommand command;

					if (delayQueue.TryDequeue(out command))
					{
						if (maxCommandsInQueue > 0)
						{
							Interlocked.Decrement(ref delayQueueCount);
						}

						try
						{
							command.ExecuteAsync(args);
						}
						catch (Exception e)
						{
							CommandFailed(command, args, e);
						}
					}
					else
					{
						argsQueue.Enqueue(args);
						break;
					}
				}
			}
			finally
			{
				if (lockTaken)
				{
					// Volatile Write. At this point, the job can be rescheduled.
					jobScheduled = 0; 
					Monitor.Exit(delayQueue);
				}
			}
		}

		private void CommandFailed(AsyncCommand command, SocketAsyncEventArgs args, Exception e)
		{
			// Restore command slot and fail command.
			argsQueue.Enqueue(args);
			command.FailOnQueueError(new AerospikeException("Failed to queue delayed async command", e));
		}
	}
}
