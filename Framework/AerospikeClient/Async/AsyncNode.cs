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
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous server node representation.
	/// </summary>
	public sealed class AsyncNode : Node
	{
		private readonly Pool<AsyncConnection> asyncConnQueue;
		private readonly new AsyncCluster cluster;
		private int asyncConnsOpened;
		private int asyncConnsClosed;

		/// <summary>
		/// Initialize server node with connection parameters.
		/// </summary>
		/// <param name="cluster">collection of active server nodes</param>
		/// <param name="nv">connection parameters</param>
		public AsyncNode(AsyncCluster cluster, NodeValidator nv) 
			: base(cluster, nv)
		{
			this.cluster = cluster;
			asyncConnQueue = new Pool<AsyncConnection>(cluster.asyncMinConnsPerNode, cluster.asyncMaxConnsPerNode);
		}

		public override void CreateMinConnections()
		{
			base.CreateMinConnections();

			// Create async connections.
			if (cluster.asyncMinConnsPerNode > 0)
			{
				new AsyncConnectorExecutor(cluster, this, cluster.asyncMinConnsPerNode, 20, true);
			}
		}

		public AsyncConnection CreateAsyncConnection(IAsyncCommand command)
		{
			if (cluster.UseTls())
			{
				return new AsyncConnectionTls(this, command);
			}
			else
			{
				return new AsyncConnectionArgs(this, command);
			}
		}

		/// <summary>
		/// Get asynchronous socket connection from connection pool for the server node.
		/// </summary>
		public AsyncConnection GetAsyncConnection()
		{
			// Try to find connection in pool.
			AsyncConnection conn = null;

			while (asyncConnQueue.TryDequeue(out conn))
			{
				if (! cluster.IsConnCurrentTran(conn.LastUsed))
				{
					CloseAsyncConn(conn);
					continue;
				}

				if (! conn.IsValid())
				{
					CloseAsyncConnOnError(conn);
					continue;
				}
				return conn;
			}
			return null;
		}

		/// <summary>
		/// Put asynchronous connection back into connection pool.
		/// </summary>
		/// <param name="conn">socket connection</param>
		public void PutAsyncConnection(AsyncConnection conn)
		{
			conn.Reset();

			if (! (active && asyncConnQueue.Enqueue(conn)))
			{
				CloseAsyncConn(conn);
			}
		}

		public override void BalanceConnections()
		{
			base.BalanceConnections();

			int excess = asyncConnQueue.Excess();

			if (excess > 0)
			{
				CloseIdleAsyncConnections(excess);
			}
			else if (excess < 0 && ErrorCountWithinLimit())
			{
				// Create connection requests sequentially because they will be done in the
				// background and there is no immediate need for them to complete.
				new AsyncConnectorExecutor(cluster, this, -excess, 1, false);
			}
		}

		private void CloseIdleAsyncConnections(int count)
		{
			while (count > 0)
			{
				AsyncConnection conn;

				if (!asyncConnQueue.TryDequeueLast(out conn))
				{
					break;
				} 
				
				if (cluster.IsConnCurrentTrim(conn.LastUsed))
				{
					if (!asyncConnQueue.EnqueueLast(conn))
					{
						CloseAsyncConn(conn);
					}
					break;
				}
				CloseAsyncConn(conn);
				count--;
			}
		}

		/// <summary>
		/// Close all asynchronous connections in the pool.
		/// </summary>
		protected internal override void CloseConnections()
		{
			base.CloseConnections();

			AsyncConnection conn;
			while (asyncConnQueue.TryDequeue(out conn))
			{
				conn.Close();
			}
		}

		internal void CloseAsyncConnOnError(AsyncConnection conn)
		{
			IncrErrorCount();
			CloseAsyncConn(conn);
		}

		private void CloseAsyncConn(AsyncConnection conn)
		{
			DecrAsyncConnTotal();
			IncrAsyncConnClosed();
			conn.Close();
		}

		internal void IncrAsyncConnTotal()
		{
			if (asyncConnQueue.IncrTotal() > asyncConnQueue.Capacity)
			{
				asyncConnQueue.DecrTotal();
				throw new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS,
					"Async max connections " + cluster.asyncMaxConnsPerNode + " would be exceeded.");
			}
		}

		internal void DecrAsyncConnTotal()
		{
			asyncConnQueue.DecrTotal();
		}

		internal void IncrAsyncConnOpened()
		{
			Interlocked.Increment(ref asyncConnsOpened);
		}

		internal void IncrAsyncConnClosed()
		{
			Interlocked.Increment(ref asyncConnsClosed);
		}

		public ConnectionStats GetAsyncConnectionStats()
		{
			int inPool = asyncConnQueue.Count;
			int inUse = asyncConnQueue.Total - inPool;

			// Timing issues may cause values to go negative. Adjust.
			if (inUse < 0)
			{
				inUse = 0;
			}
			return new ConnectionStats(inPool, inUse, asyncConnsOpened, asyncConnsClosed);
		}
	}
}
