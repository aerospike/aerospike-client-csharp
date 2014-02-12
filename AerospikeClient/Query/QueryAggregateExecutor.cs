/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class QueryAggregateExecutor : QueryExecutor
	{
		private readonly Node[] nodes;
		private readonly BlockingCollection<object> inputQueue;
		private readonly ResultSet resultSet;
		private readonly Thread luaThread;

		public QueryAggregateExecutor(QueryPolicy policy, Statement statement, Node[] nodes, string packageName, string functionName, Value[] functionArgs) 
			: base(policy, statement)
		{
			this.nodes = nodes;
			inputQueue = new BlockingCollection<object>(500);
			resultSet = new ResultSet(this, policy.recordQueueSize);
			statement.SetAggregateFunction(packageName, functionName, functionArgs, true);

			// Start Lua thread which reads from a queue, applies aggregate function and 
			// writes to a result set. 
			luaThread = new Thread(new ThreadStart(Run));
			luaThread.Start();
		}

		public void Run()
		{
			try
			{
				RunThreads();
			}
			catch (Exception e)
			{
				StopThreads(e);
			}
		}

		public void RunThreads()
		{
			LuaInstance lua = LuaCache.GetInstance();

			// Start thread queries to each node.
			StartThreads(nodes);

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
					luaThread.Join(1000);
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
