/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Runtime.ConstrainedExecution;
using System.Text;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Cluster statistics.
	/// </summary>
	public sealed class ClusterStats
	{
		/// <summary>
		/// Statistics for each node.
		/// </summary>
		public readonly NodeStats[] nodes;

		/// <summary>
		/// Number of active threads executing sync batch/scan/query commands.
		/// </summary>
		public readonly int threadsInUse;

		/// <summary>
		/// Number of active async completion ports.
		/// </summary>
		public readonly int completionPortsInUse;

		/// <summary>
		/// Count of add node failures in the most recent cluster tend iteration.
		/// </summary>
		public readonly int invalidNodeCount;

		/// <summary>
		/// Count of transaction retires since cluster was started.
		/// </summary>
		public readonly long retryCount;

		/// <summary>
		/// Cluster statistics constructor.
		/// </summary>
		public ClusterStats(Cluster cluster, NodeStats[] nodes)
		{
			this.nodes = nodes;
			this.invalidNodeCount = cluster.InvalidNodeCount;
			this.retryCount = cluster.GetRetryCount();

			int workerThreadsMax;
			int completionPortThreadsMax;
			ThreadPool.GetMaxThreads(out workerThreadsMax, out completionPortThreadsMax);

			int workerThreads;
			int completionPortThreads;
			ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);

			this.threadsInUse = workerThreadsMax - workerThreads;
			this.completionPortsInUse = completionPortThreadsMax - completionPortThreads;
        }

		/// <summary>
		/// Convert statistics to string.
		/// </summary>
		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(1024);
			sb.Append("nodes(inUse,inPool,opened,closed):");
			sb.Append(System.Environment.NewLine);

			foreach (NodeStats stat in nodes)
			{
				sb.Append(stat);
				sb.Append(System.Environment.NewLine);
			}

			sb.Append("threadsInUse: " + threadsInUse);
			sb.Append(System.Environment.NewLine);
			sb.Append("completionPortsInUse: " + completionPortsInUse);
			sb.Append(System.Environment.NewLine);
			sb.Append("invalidNodeCount: " + invalidNodeCount);
			sb.Append(System.Environment.NewLine);
			sb.Append("retryCount: " + retryCount);
			return sb.ToString();
		}
	}

	/// <summary>
	/// Node statistics.
	/// </summary>
	public sealed class NodeStats
	{
		/// <summary>
		/// Node for which statistics are generated.
		/// </summary>
		public readonly Node node;

		/// <summary>
		/// Connection statistics for sync commands.
		/// </summary>
		public readonly ConnectionStats syncStats;

		/// <summary>
		/// Connection statistics for async commands.
		/// </summary>
		public readonly ConnectionStats asyncStats;

		/// <summary>
		/// Transaction error count since node was initialized. If the error is retryable, multiple errors per
		/// transaction may occur.
		/// </summary>
		public readonly long errorCount;

		/// <summary>
		/// Transaction timeout count since node was initialized. If the timeout is retryable (ie socketTimeout),
		/// multiple timeouts per transaction may occur.
		/// </summary>
		public readonly long timeoutCount;

		/// <summary>
		/// Node statistics constructor.
		/// </summary>
		public NodeStats(Node node)
		{
			this.node = node;
			this.syncStats = node.GetConnectionStats();
			this.errorCount = node.GetErrorCount();
			this.timeoutCount = node.GetTimeoutCount();

			if (node is AsyncNode)
			{
				this.asyncStats = ((AsyncNode)node).GetAsyncConnectionStats();
			}
			else
			{
				this.asyncStats = new ConnectionStats(0, 0, 0, 0);
			}
		}

		/// <summary>
		/// Convert statistics to string.
		/// </summary>
		public override string ToString()
		{
			return node + " sync(" + syncStats + ") async(" + asyncStats + ") " + errorCount + ',' + timeoutCount;
		}
	}

	/// <summary>
	/// Connection statistics.
	/// </summary>
	public sealed class ConnectionStats
	{
		/// <summary>
		/// Connections residing in connection pool(s).
		/// </summary>
		public readonly int inPool;

		/// <summary>
		/// Active connections in currently executing commands.
		/// </summary>
		public readonly int inUse;

		/// <summary>
		/// Total number of node connections opened since node creation.
		/// </summary>
		public readonly int opened;

		/// <summary>
		/// Total number of node connections closed since node creation.
		/// </summary>
		public readonly int closed;

		/// <summary>
		/// Total number of bytes received from that connection.
		/// </summary>
		public readonly long bytesReceived;

		/// <summary>
		/// Total number of bytes sent to that connection.
		/// </summary>
		public readonly long bytesSent;

		/// <summary>
		/// Connection statistics constructor.
		/// </summary>
		public ConnectionStats(int inPool, int inUse, int opened, int closed)
		{
			this.inPool = inPool;
			this.inUse = inUse;
			this.opened = opened;
			this.closed = closed;
			this.bytesReceived = -1;
			this.bytesSent = -1;
		}

		/// <summary>
		/// Connection statistics constructor, including byte metrics.
		/// </summary>
		public ConnectionStats(int inPool, int inUse, int opened, int closed, long bytesReceived, long bytesSent)
		{
			this.inPool = inPool;
			this.inUse = inUse;
			this.opened = opened;
			this.closed = closed;
			this.bytesReceived = bytesReceived;
			this.bytesSent = bytesSent;
		}

		/// <summary>
		/// Convert statistics to string.
		/// </summary>
		public override string ToString()
		{
			if (this.bytesReceived < 0)
			{
				return "" + inUse + ',' + inPool + ',' + opened + ',' + closed;
			}
			else
			{
				return "" + inUse + ',' + inPool + ',' + opened + ',' + closed + ',' + bytesReceived + ',' + bytesSent;
			}
		}
	}
}
