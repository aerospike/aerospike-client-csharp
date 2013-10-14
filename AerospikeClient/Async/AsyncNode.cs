/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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