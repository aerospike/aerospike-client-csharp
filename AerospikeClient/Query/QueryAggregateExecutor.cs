/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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