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
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	/// <summary>
	/// Async command scheduling interface.
	/// </summary>
	public interface AsyncScheduler
	{
		void Schedule(AsyncCommand command);
		void Release(BufferSegment segment);
	}

	/// <summary>
	/// Put command on delay queue for later execution if capacity has been reached.
	/// </summary>
	public sealed class DelayScheduler : AsyncScheduler
	{
		private readonly ConcurrentQueue<BufferSegment> bufferQueue = new ConcurrentQueue<BufferSegment>();
		private readonly ConcurrentQueue<AsyncCommand> delayQueue = new ConcurrentQueue<AsyncCommand>();
		private readonly WaitCallback jobCallback;
		private readonly int delayQueueMax;
		private int delayQueueCount;
		private volatile int jobScheduled;

		public DelayScheduler(AsyncClientPolicy policy, BufferPool pool)
		{
			jobCallback = ProcessQueueExclusive;
			delayQueueMax = policy.asyncMaxCommandsInQueue;

			for (int i = 0; i < policy.asyncMaxCommands; i++)
			{
				bufferQueue.Enqueue(new BufferSegment(pool, i));
			}
		}

		/// <summary>
		/// Schedule command for execution.
		/// </summary>
		public void Schedule(AsyncCommand command)
		{
			// Check if command slot is available.
			if (bufferQueue.TryDequeue(out BufferSegment segment))
			{
				// Command slot is available. Check delay queue.
				if (delayQueue.TryDequeue(out AsyncCommand delayedCommand))
				{
					// Commands in delay queue take precedence over new commands.
					// Put new command in delay queue and use command slot to execute 
					// delayedCommand. delayQueueCount remains the same. 
					delayQueue.Enqueue(command);

					try
					{
						delayedCommand.ExecuteAsync(segment);
					}
					catch (Exception e)
					{
						CommandFailed(delayedCommand, segment, e);
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
					command.ExecuteInline(segment);
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
				if (delayQueueMax == 0 || Interlocked.Increment(ref delayQueueCount) <= delayQueueMax)
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

		/// <summary>
		/// Release command slot.
		/// </summary>
		public void Release(BufferSegment segment)
		{
			// Check delay queue.
			if (delayQueue.TryDequeue(out AsyncCommand command))
			{
				// Use command slot to execute delayed command.
				if (delayQueueMax > 0)
				{
					Interlocked.Decrement(ref delayQueueCount);
				}

				try
				{
					command.ExecuteAsync(segment);
				}
				catch (Exception e)
				{
					CommandFailed(command, segment, e);
				}
				finally
				{
					if (!bufferQueue.IsEmpty)
					{
						ProcessDelayQueue();
					}
				}
			}
			else
			{
				// Put command slot back into pool.
				// Do not process delay queue.
				bufferQueue.Enqueue(segment);
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

		private void ProcessQueueExclusive(object state)
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
				while (!delayQueue.IsEmpty && bufferQueue.TryDequeue(out BufferSegment segment))
				{
					if (delayQueue.TryDequeue(out AsyncCommand command))
					{
						if (delayQueueMax > 0)
						{
							Interlocked.Decrement(ref delayQueueCount);
						}

						try
						{
							command.ExecuteAsync(segment);
						}
						catch (Exception e)
						{
							CommandFailed(command, segment, e);
						}
					}
					else
					{
						bufferQueue.Enqueue(segment);
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

		private void CommandFailed(AsyncCommand command, BufferSegment segment, Exception e)
		{
			// Restore command slot and fail command.
			bufferQueue.Enqueue(segment);
			command.FailOnQueueError(new AerospikeException("Failed to queue delayed async command", e));
		}
	}

	/// <summary>
	/// Reject command if capacity has been reached.
	/// </summary>
	public sealed class RejectScheduler : AsyncScheduler
	{
		private readonly ConcurrentQueue<BufferSegment> bufferQueue = new ConcurrentQueue<BufferSegment>();

		public RejectScheduler(AsyncClientPolicy policy, BufferPool pool)
		{
			for (int i = 0; i < policy.asyncMaxCommands; i++)
			{
				bufferQueue.Enqueue(new BufferSegment(pool, i));
			}
		}

		/// <summary>
		/// Reserve command slot. Reject command if at capacity.
		/// </summary>
		public void Schedule(AsyncCommand command)
		{
			// Try to dequeue one SocketAsyncEventArgs object from the queue and execute the command.
			if (bufferQueue.TryDequeue(out BufferSegment segment))
			{
				command.ExecuteInline(segment);
			}
			else
			{
				// Queue is empty. Reject command.
				throw new AerospikeException.CommandRejected();
			}
		}

		/// <summary>
		/// Release command slot.
		/// </summary>
		public void Release(BufferSegment segment)
		{
			bufferQueue.Enqueue(segment);
		}
	}

	/// <summary>
	/// Block until command slot is available.
	/// </summary>
	public sealed class BlockScheduler : AsyncScheduler
	{
		private readonly BlockingCollection<BufferSegment> bufferQueue = new BlockingCollection<BufferSegment>();

		public BlockScheduler(AsyncClientPolicy policy, BufferPool pool)
		{
			for (int i = 0; i < policy.asyncMaxCommands; i++)
			{
				bufferQueue.Add(new BufferSegment(pool, i));
			}
		}

		/// <summary>
		/// Wait for command slot(s). Execute command when command slot is available.
		/// </summary>
		public void Schedule(AsyncCommand command)
		{
			// Block until buffer becomes available.
			command.ExecuteInline(bufferQueue.Take());
		}

		/// <summary>
		/// Release command slot.
		/// </summary>
		public void Release(BufferSegment segment)
		{
			bufferQueue.Add(segment);
		}
	}
}
