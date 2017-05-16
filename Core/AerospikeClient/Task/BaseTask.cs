/* 
 * Copyright 2012-2017 Aerospike, Inc.
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

namespace Aerospike.Client
{
	/// <summary>
	/// Task used to poll for server task completion.
	/// </summary>
	public abstract class BaseTask
	{
		public const int NOT_FOUND = 0;
		public const int IN_PROGRESS = 1;
		public const int COMPLETE = 2; 

		protected internal readonly Cluster cluster;
		protected internal InfoPolicy policy;
		private bool done;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public BaseTask(Cluster cluster, Policy policy)
		{
			this.cluster = cluster;
			this.policy = new InfoPolicy(policy);
			this.done = false;
		}

		/// <summary>
		/// Initialize task that has already completed.
		/// </summary>
		public BaseTask()
		{
			this.cluster = null;
			this.policy = null;
			this.done = true;
		}

		/// <summary>
		/// Wait for asynchronous task to complete using default sleep interval (1 second).
		/// The timeout is passed from the original task policy. If task is not complete by timeout,
		/// an exception is thrown.  Do not timeout if timeout set to zero.
		/// </summary>
		public void Wait()
		{
			TaskWait(1000);
		}

		/// <summary>
		/// Wait for asynchronous task to complete using given sleep interval in milliseconds.
		/// The timeout is passed from the original task policy. If task is not complete by timeout,
		/// an exception is thrown.  Do not timeout if policy timeout set to zero.
		/// </summary>
		public void Wait(int sleepInterval)
		{
			TaskWait(sleepInterval);
		}

		/// <summary>
		/// Wait for asynchronous task to complete using given sleep interval and timeout in milliseconds.
		/// If task is not complete by timeout, an exception is thrown.  Do not timeout if timeout set to
		/// zero.
		/// </summary>
		public void Wait(int sleepInterval, int timeout)
		{
			policy = new InfoPolicy();
			policy.timeout = timeout;
			TaskWait(sleepInterval);
		}

		/// <summary>
		/// Wait for asynchronous task to complete using given sleep interval in milliseconds.
		/// The timeout is passed from the original task policy. If task is not complete by timeout,
		/// an exception is thrown.  Do not timeout if policy timeout set to zero.
		/// </summary>
		private void TaskWait(int sleepInterval)
		{
			if (done)
			{
				return;
			}

			DateTime deadline = DateTime.UtcNow.AddMilliseconds(policy.timeout);

			do
			{
				// Sleep first to give task a chance to complete and help avoid case
				// where task hasn't started yet.
				Util.Sleep(sleepInterval);

				int status = QueryStatus();

				// The server can remove task listings immediately after completion
				// (especially for background query execute), so "NOT_FOUND" can 
				// really mean complete. If not found and timeout not defined,
				// consider task complete.
				if (status == COMPLETE || (status == NOT_FOUND && policy.timeout == 0))
				{
					done = true;
					return;
				}

				// Check for timeout.
				if (policy.timeout > 0 && DateTime.UtcNow.AddMilliseconds(sleepInterval) > deadline)
				{
					// Timeout has been reached or will be reached after next sleep.
					throw new AerospikeException.Timeout();
				}
			} while (true);
		}

		/// <summary>
		/// Has task completed.
		/// </summary>
		public bool IsDone()
		{
			if (done)
			{
				return true;
			}

			int status = QueryStatus();

			if (status == NOT_FOUND)
			{
				// The task may have not started yet.  Re-request status after a delay.
				Util.Sleep(1000);
				status = QueryStatus();
			}

			// The server can remove task listings immediately after completion
			// (especially for background query execute), so we must assume a 
			// "not found" status means the task is complete.
			done = status != IN_PROGRESS;
			return done; 			
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public abstract int QueryStatus();
	}
}
