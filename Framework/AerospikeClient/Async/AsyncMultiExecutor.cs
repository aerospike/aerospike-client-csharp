/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Aerospike.Client
{
	public abstract class AsyncMultiExecutor
	{
		internal readonly AsyncCluster cluster; 
		private AsyncMultiCommand[] commands;
		private string ns;
		private AerospikeException exception; 
		private ulong clusterKey;
		private int maxConcurrent;
		private int completedCount;
		private readonly bool stopOnFailure;
		private volatile int done;

		public AsyncMultiExecutor(AsyncCluster cluster)
		{
			this.cluster = cluster;
			this.stopOnFailure = true;
		}

		public AsyncMultiExecutor(AsyncCluster cluster, bool stopOnFailure)
		{
			this.cluster = cluster;
			this.stopOnFailure = stopOnFailure;
		}

		public void Execute(AsyncMultiCommand[] commands, int maxConcurrent)
		{
			this.commands = commands;
			this.maxConcurrent = (maxConcurrent == 0 || maxConcurrent >= commands.Length) ? commands.Length : maxConcurrent;

			for (int i = 0; i < this.maxConcurrent; i++)
			{
				commands[i].Execute();
			}
		}

		public void ExecuteBatchRetry(AsyncMultiCommand[] cmds, AsyncMultiCommand orig)
		{
			// Create new commands array.
			List<AsyncMultiCommand> target = new List<AsyncMultiCommand>();

			foreach (AsyncMultiCommand cmd in commands)
			{
				if (cmd != orig)
				{
					target.Add(cmd);
				}
			}

			foreach (AsyncMultiCommand cmd in cmds)
			{
				target.Add(cmd);
			}
			commands = target.ToArray();

			// Batch executors always execute all commands at once.
			// Execute all new commands.
			maxConcurrent = commands.Length;

			foreach (AsyncMultiCommand cmd in cmds)
			{
				cmd.Execute();
			}
		}

		public void ExecuteValidate(AsyncMultiCommand[] commands, int maxConcurrent, string ns)
		{
			this.commands = commands;
			this.maxConcurrent = (maxConcurrent == 0 || maxConcurrent >= commands.Length) ? commands.Length : maxConcurrent;
			this.ns = ns;

			AsyncQueryValidate.ValidateBegin(cluster, new BeginHandler(this, commands, this.maxConcurrent), commands[0].node, ns);
		}

		public bool IsDone()
		{
			return done != 0;
		}

		private class BeginHandler : AsyncQueryValidate.BeginListener
		{
			private readonly AsyncMultiExecutor parent;
			private AsyncMultiCommand[] commands;
			private int max;

			public BeginHandler(AsyncMultiExecutor parent, AsyncMultiCommand[] commands, int max)
			{
				this.parent = parent;
				this.commands = commands;
				this.max = max;
			}

			public void OnSuccess(ulong key)
			{
				parent.clusterKey = key;
				commands[0].Execute();

				for (int i = 1; i < max; i++)
				{
					parent.ExecuteValidateCommand(commands[i]);
				}
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.OnFailure(ae);
			}
		}

		private void ExecuteValidateCommand(AsyncMultiCommand command)
		{
			AsyncQueryValidate.Validate(cluster, new NextHandler(this, command), command.node, ns, clusterKey);
		}

		private class NextHandler : AsyncQueryValidate.Listener
		{
			private readonly AsyncMultiExecutor parent;

			private AsyncMultiCommand command;

			public NextHandler(AsyncMultiExecutor parent, AsyncMultiCommand command)
			{
				this.parent = parent;
				this.command = command;
			}

			public void OnSuccess()
			{
				command.Execute();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.ChildFailure(ae);
			}
		}

		protected internal void ChildSuccess(AsyncNode node)
		{
			if (clusterKey == 0)
			{
				QueryComplete();
			}
			else
			{
				AsyncQueryValidate.Validate(cluster, new EndHandler(this), node, ns, clusterKey);
			}
		}

		private class EndHandler : AsyncQueryValidate.Listener
		{
			private readonly AsyncMultiExecutor parent;

			public EndHandler(AsyncMultiExecutor parent)
			{
				this.parent = parent;
			}

			public void OnSuccess()
			{
				parent.QueryComplete();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.ChildFailure(ae);
			}
		}
		
		private void QueryComplete()
		{
			int finished = Interlocked.Increment(ref completedCount);

			if (finished < commands.Length)
			{
				int nextThread = finished + maxConcurrent - 1;

				// Determine if a new command needs to be started.
				if (nextThread < commands.Length && done == 0)
				{
					// Start new command.
					if (clusterKey == 0)
					{
						commands[nextThread].Execute();
					}
					else
					{
						ExecuteValidateCommand(commands[nextThread]);
					}
				}
			}
			else
			{
				// All commands complete. Notify success if an exception has not already occurred.
				int status = Interlocked.CompareExchange(ref done, 1, 0);

				if (status == 0)
				{
					if (exception == null)
					{
						OnSuccess();
					}
					else
					{
						OnFailure(exception);
					}
				}
			}
		}

		internal void ChildFailure(AerospikeException ae)
		{
			if (stopOnFailure)
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
			else
			{
				// Batch sequence executors continue processing.
				if (exception == null)
				{
					exception = ae;
				}
				QueryComplete();
			}
		}

		internal void Reset()
		{
			this.completedCount = 0;
			this.done = 0;
		}
		
		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
