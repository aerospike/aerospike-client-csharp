/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Threading;

namespace Aerospike.Client
{
	public abstract class AsyncMultiExecutor
	{
		private int completedCount;
		private int done;
		private AsyncMultiCommand[] commands;
		private int maxConcurrent;

		public void Execute(AsyncMultiCommand[] commands, int maxConcurrent)
		{
			this.commands = commands;
			this.maxConcurrent = (maxConcurrent == 0 || maxConcurrent >= commands.Length) ? commands.Length : maxConcurrent;

			for (int i = 0; i < this.maxConcurrent; i++)
			{
				commands[i].Execute();
			}
		}

		protected internal void ChildSuccess()
		{
			int finished = Interlocked.Increment(ref completedCount);

			if (finished < commands.Length)
			{
				int nextThread = finished + maxConcurrent - 1;

				// Determine if a new command needs to be started.
				if (nextThread < commands.Length && done == 0)
				{
					// Start new command.
					commands[nextThread].Execute();
				}
			}
			else
			{
				// All commands complete. Notify success if an exception has not already occurred.
				int status = Interlocked.CompareExchange(ref done, 1, 0);

				if (status == 0)
				{
					OnSuccess();
				}
			}
		}

		protected internal void ChildFailure(AerospikeException ae)
		{
			// There is no need to stop commands if all commands have already completed.
			int status = Interlocked.CompareExchange(ref done, 1, 0);

			if (status == 0)
			{    	
				// Send stop signal to all commands.
				foreach (AsyncMultiCommand command in commands)
				{
					command.Stop();
				}
				OnFailure(ae);
			}
		}

		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
