/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Async command scheduling interface.
	/// </summary>
	public interface AsyncScheduler
	{
		void Schedule(AsyncCommand cmd);
		void Release();
	}

	/// <summary>
	/// Put command on delay queue for later execution if capacity has been reached.
	/// </summary>
	public sealed class DelayScheduler : AsyncScheduler
	{
		private readonly ConcurrentQueue<AsyncCommand> queue = new ConcurrentQueue<AsyncCommand>();
		private readonly int queueMax;
		private int queueCount;
		private int available;
		private volatile int jobScheduled;
		private readonly WaitCallback jobCallback;

		public DelayScheduler(int capacity)
		{
			available = capacity;
			jobCallback = new WaitCallback(ProcessQueueExclusive);
		}

		public void ScheduleCommand(AsyncCommand cmd)
		{
			if (Interlocked.Decrement(ref available) >= 0)
			{
				// Command slot is available. Check delay queue.
				AsyncCommand delayed;

				if (queue.TryDequeue(out delayed))
				{
					// Commands in delay queue take precedence over new commands.
					// Put new command in delay queue and use command slot to execute 
					// delayed command. delayQueueCount remains the same. 
					queue.Enqueue(cmd);

					try
					{
						delayed.ExecuteAsync();
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
					cmd.ExecuteInline();
				}
			}
			else
			{
				// Command slot not available.
				Interlocked.Increment(ref available);

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
				// maxCommandsInQueue values should be at least 1000 when used. Thus, the more 
				// performant solution was chosen.
				if (queueMax == 0 || Interlocked.Increment(ref queueCount) <= queueMax)
				{
					// Add command to delay queue. There are no available command slots, so do not
					// process delay queue further.
					queue.Enqueue(cmd);
				}
				else
				{
					Interlocked.Decrement(ref queueCount);
					throw new AerospikeException.CommandRejected();
				}
			}
		}

		/// <summary>
		/// Recover slot(s) from commands that have completed.
		/// </summary>
		public void Release()
		{
			AsyncCommand cmd;

			// Check delay queue.
			if (queue.TryDequeue(out cmd))
			{
				// Use command slot to execute delayed command.
				if (queueMax > 0)
				{
					Interlocked.Decrement(ref queueCount);
				}

				try
				{
					cmd.ExecuteAsync();
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
				// Make command slot available.
				Interlocked.Increment(ref available);
			}
		}

		private void ProcessDelayQueue()
		{
			// Schedule exactly once the job that will execute delayed commands.
			if (Interlocked.CompareExchange(ref jobScheduled, 1, 0) == 0)
			{
				ThreadPool.UnsafeQueueUserWorkItem(jobCallback, null);
			}
		}

		private void ProcessDelayQueueExclusive(object state)
		{
			// Schedule as many commands as possible.
			bool lockTaken = false;

			try
			{
				// Lock on delayQueue for exclusive execution of the job.
				Monitor.TryEnter(queue, ref lockTaken);

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
					Monitor.Exit(queue);
				}
			}
		}

		/// <summary>
		/// Reject command if capacity has been reached.
		/// </summary>
		public sealed class RejectScheduler : AsyncScheduler
	{
		private int available;

		public RejectScheduler(int capacity)
		{
			available = capacity;
		}

		/// <summary>
		/// Reserve commands. Reject command if at capacity.
		/// </summary>
		public void Schedule(AsyncCommand cmd)
		{
			if (Interlocked.Decrement(ref available) < 0)
			{
				Interlocked.Increment(ref available);
				throw new AerospikeException.CommandRejected();
			}
			cmd.ExecuteInline();
		}

		/// <summary>
		/// Recover slot(s) from commands that have completed.
		/// </summary>
		public void Release()
		{
			Interlocked.Increment(ref available);
		}
	}

	/// <summary>
	/// Block until command slot is available.
	/// </summary>
	public sealed class BlockScheduler : AsyncScheduler
	{
		private int available;

		public BlockScheduler(int capacity)
		{
			available = capacity;
		}

		/// <summary>
		/// Wait for command slot(s). Execute command when command slot is available.
		/// </summary>
		public void Schedule(AsyncCommand cmd)
		{
			lock (this)
			{
				while (available <= 0)
				{
					Monitor.Wait(this);
				}
				available--;
			}
			cmd.ExecuteInline();
		}

		/// <summary>
		/// Release command slot.
		/// </summary>
		public void Release()
		{
			lock (this)
			{
				available++;
				Monitor.Pulse(this);
			}
		}
	}
}
