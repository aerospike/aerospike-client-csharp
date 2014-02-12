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
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous server node representation.
	/// </summary>
	public sealed class AsyncNode : Node
	{
		private readonly BlockingCollection<AsyncConnection> asyncConnQueue;
		private readonly new AsyncCluster cluster;

		/// <summary>
		/// Initialize server node with connection parameters.
		/// </summary>
		/// <param name="cluster">collection of active server nodes</param>
		/// <param name="nv">connection parameters</param>
		public AsyncNode(AsyncCluster cluster, NodeValidator nv) 
			: base(cluster, nv)
		{
			this.cluster = cluster;
			asyncConnQueue = new BlockingCollection<AsyncConnection>(cluster.MaxCommands);
		}

		/// <summary>
		/// Get asynchronous socket connection from connection pool for the server node.
		/// </summary>
		public AsyncConnection GetAsyncConnection()
		{
			// Try to find connection in pool.
			AsyncConnection conn = null;

			while (asyncConnQueue.TryTake(out conn))
			{
				if (conn.IsValid())
				{
					return conn;
				}
				conn.Close();
			}
			return null;
		}

		/// <summary>
		/// Put asynchronous connection back into connection pool.
		/// </summary>
		/// <param name="conn">socket connection</param>
		public void PutAsyncConnection(AsyncConnection conn)
		{
			if (!active || !asyncConnQueue.TryAdd(conn))
			{
				conn.Close();
			}
		}

		/// <summary>
		/// Close all asynchronous connections in the pool.
		/// </summary>
		protected internal override void CloseConnections()
		{
			base.CloseConnections();

			AsyncConnection conn;
			while (asyncConnQueue.TryTake(out conn))
			{
				conn.Close();
			}
		}
	}
}
