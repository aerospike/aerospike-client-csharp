/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
	/// Asynchronous server node representation.
	/// </summary>
	public sealed class AsyncNode : Node
	{
		private readonly ConcurrentQueue<AsyncConnection> asyncConnQueue;
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
			asyncConnQueue = new ConcurrentQueue<AsyncConnection>();
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
			if (active)
			{
				asyncConnQueue.Enqueue(conn);
			}
			else
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
			while (asyncConnQueue.TryDequeue(out conn))
			{
				conn.Close();
			}
		}
	}
}
