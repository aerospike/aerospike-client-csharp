/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
	public sealed class QueryAggregateExecutor : QueryExecutor
	{
		private readonly BlockingCollection<object> inputQueue;
		private readonly ResultSet resultSet;
		private readonly ManualResetEvent resetEvent;

		public QueryAggregateExecutor
		(
			Cluster cluster,
			QueryPolicy policy,
			Statement statement,
			string packageName,
			string functionName,
			Value[] functionArgs
		) : base(cluster, policy, statement)
		{
			inputQueue = new BlockingCollection<object>(500);
			resultSet = new ResultSet(this, policy.recordQueueSize);
			statement.SetAggregateFunction(packageName, functionName, functionArgs, true);
			resetEvent = new ManualResetEvent(false);
		}

		public void Execute()
		{
			// Start Lua thread which reads from a queue, applies aggregate function and 
			// writes to a result set.
			ThreadPool.QueueUserWorkItem(this.Run);
		}

		public void Run(object obj)
		{
			try
			{
				RunThreads();
			}
			catch (Exception e)
			{
				StopThreads(e);
			}
			resetEvent.Set();
		}

		public void RunThreads()
		{
			LuaInstance lua = LuaCache.GetInstance();

			// Start thread queries to each node.
			StartThreads();

			try
			{
				lua.Load(statement.packageName);

				object[] args = new object[4 + statement.functionArgs.Length];
				args[0] = lua.GetFunction(statement.functionName);
				args[1] = 2;
				args[2] = new LuaInputStream(inputQueue);
				args[3] = new LuaOutputStream(resultSet);
				int count = 4;

				foreach (Value value in statement.functionArgs)
				{
					args[count++] = value.Object;
				}
				lua.Call("apply_stream", args);
			}
			finally
			{
				LuaCache.PutInstance(lua);
			}
		}

		protected internal override QueryCommand CreateCommand(Node node)
		{
			return new QueryAggregateCommand(node, policy, statement, inputQueue);
		}

		protected internal override void SendCompleted()
		{
			try
			{
				// Send end command to lua thread.
				inputQueue.Add(null);

				// Ensure lua thread completes before sending end command to result set.
				if (exception == null)
				{
					resetEvent.WaitOne(1000);
				}
			}
			catch (ThreadInterruptedException)
			{
			}

			// Send end command to user's result set.
			resultSet.Put(ResultSet.END);
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
