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
using System;
using System.Collections.Concurrent;
using System.Threading;
using Neo.IronLua;

namespace Aerospike.Client
{
	public sealed class QueryAggregateExecutor : QueryExecutor
	{
		private readonly BlockingCollection<object> inputQueue;
		private readonly ResultSet resultSet;

		public QueryAggregateExecutor(Cluster cluster, QueryPolicy policy, Statement statement) 
			: base(cluster, policy, statement)
		{
			inputQueue = new BlockingCollection<object>(500);
			resultSet = new ResultSet(this, policy.recordQueueSize, cancel.Token);
			InitializeThreads();
		}

		public void Execute()
		{
			// Start Lua thread which reads from a queue, applies aggregate function and 
			// writes to a result set.
			ThreadPool.QueueUserWorkItem(this.Run);
		}

		public void Run(object obj)
		{
			LuaInstance lua = null;

			try
			{
				lua = LuaCache.GetInstance();

				// Start thread queries to each node.
				StartThreads();

				lua.LoadPackage(statement);

				object[] args = new object[4 + statement.functionArgs.Length];
				args[0] = lua.GetFunction(statement.functionName);
				args[1] = 2;
				args[2] = new LuaInputStream(inputQueue, cancel.Token);
				args[3] = new LuaOutputStream(resultSet);
				int count = 4;

				foreach (Value value in statement.functionArgs)
				{
					args[count++] = value.Object;
				}
				lua.Call("apply_stream", args);
			}
			catch (LuaRuntimeException lre)
			{
				// Try to get that elusive lua stack trace.
				HandleException(new Exception(lre.Message + lre.StackTrace));
			}
			catch (Exception e)
			{
				if (e.InnerException is LuaRuntimeException)
				{
					// Try to get that elusive lua stack trace.
					LuaRuntimeException lre = e.InnerException as LuaRuntimeException;
					HandleException(new Exception(lre.Message + lre.StackTrace));
				}
				else
				{
					HandleException(e);
				}
			}
			finally
			{
				// Send end command to user's result set.
				// If query was already cancelled, this put will be ignored.
				resultSet.Put(ResultSet.END);

				if (lua != null)
				{
					LuaCache.PutInstance(lua);
				}
			}
		}

		private void HandleException(Exception e)
		{
			// Stop query threads before END is put on result set.
			if (!StopThreads(e))
			{
				// Override exception if a query thread already called StopThreads.
				base.exception = e;
			}
		}

		protected internal override MultiCommand CreateCommand(Node node)
		{
			return new QueryAggregateCommand(node, policy, statement, inputQueue, cancel.Token);
		}

		protected internal override void SendCancel()
		{
			resultSet.Abort();

			// Send end command to lua thread.
			// It's critical that the end token add succeeds.
			while (!inputQueue.TryAdd(null))
			{
				// Queue must be full. Remove one item to make room.
				object tmp;
				if (!inputQueue.TryTake(out tmp))
				{
					// Can't add or take.  Nothing can be done here.
					if (Log.DebugEnabled())
					{
						Log.Debug("Lua input queue " + statement.taskId + " both add and take failed on abort");
					}
					break;
				}
			}
		}

		protected internal override void SendCompleted()
		{
			// Send end command to lua thread.
			// It's critical that the end token add succeeds.
			inputQueue.Add(null, cancel.Token);
		}

		public ResultSet ResultSet
		{
			get
			{
				return resultSet;
			}
		}
	}
}
