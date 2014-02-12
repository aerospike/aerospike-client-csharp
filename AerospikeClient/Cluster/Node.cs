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
using System.Net;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	/// <summary>
	/// Server node representation.  This class manages server node connections and health status.
	/// </summary>
	public class Node
	{
		/// <summary>
		/// Number of partitions for each namespace.
		/// </summary>
		public const int PARTITIONS = 4096;
		private const int FULL_HEALTH = 100;

		protected internal readonly Cluster cluster;
		private readonly string name;
		private readonly Host host;
		private Host[] aliases;
		protected internal readonly IPEndPoint address;
		private readonly BlockingCollection<Connection> connectionQueue;
		private int health = FULL_HEALTH;
		private int partitionGeneration = -1;
		protected internal int referenceCount;
		protected internal bool responded;
		protected internal readonly bool useNewInfo;
		protected internal volatile bool active = true;

		/// <summary>
		/// Initialize server node with connection parameters.
		/// </summary>
		/// <param name="cluster">collection of active server nodes</param>
		/// <param name="nv">connection parameters</param>
		public Node(Cluster cluster, NodeValidator nv)
		{
			this.cluster = cluster;
			this.name = nv.name;
			this.aliases = nv.aliases;
			this.address = nv.address;
			this.useNewInfo = nv.useNewInfo;

			// Assign host to first IP alias because the server identifies nodes 
			// by IP address (not hostname). 
			this.host = aliases[0];

			connectionQueue = new BlockingCollection<Connection>(cluster.connectionQueueSize);
		}

		~Node()
		{
			// Close connections that slipped through the cracks on race conditions.
			CloseConnections();
		}

		/// <summary>
		/// Request current status from server node.
		/// </summary>
		/// <param name="friends">other nodes in the cluster, populated by this method</param>
		/// <exception cref="Exception">if status request fails</exception>
		public void Refresh(List<Host> friends)
		{
			Connection conn = GetConnection(1000);

			try
			{
				Dictionary<string, string> infoMap = Info.Request(conn, "node", "partition-generation", "services");
				VerifyNodeName(infoMap);
				RestoreHealth();
				responded = true;
				AddFriends(infoMap, friends);
				UpdatePartitions(conn, infoMap);
				PutConnection(conn);
			}
			catch (Exception)
			{
				conn.Close();
				DecreaseHealth();
				throw;
			}
		}

		private void VerifyNodeName(Dictionary<string, string> infoMap)
		{
			// If the node name has changed, remove node from cluster and hope one of the other host
			// aliases is still valid.  Round-robbin DNS may result in a hostname that resolves to a
			// new address.
			string infoName = infoMap["node"];

			if (infoName == null || infoName.Length == 0)
			{
				DecreaseHealth();
				throw new AerospikeException.Parse("Node name is empty");
			}

			if (!name.Equals(infoName))
			{
				// Set node to inactive immediately.
				Log.Info("NAME ERROR");
				active = false;
				throw new AerospikeException("Node name has changed. Old=" + name + " New=" + infoName);
			}
		}

		private void AddFriends(Dictionary<string, string> infoMap, List<Host> friends)
		{
			// Parse the service addresses and add the friends to the list.
			string friendString = infoMap["services"];

			if (friendString == null || friendString.Length == 0)
			{
				return;
			}

			string[] friendNames = friendString.Split(';');

			foreach (string friend in friendNames)
			{
				string[] friendInfo = friend.Split(':');
				string host = friendInfo[0];
				string alternativeHost;

				if (cluster.ipMap != null && cluster.ipMap.TryGetValue(host, out alternativeHost))
				{
					host = alternativeHost;
				}

				int port = Convert.ToInt32(friendInfo[1]);
				Host alias = new Host(host, port);
				Node node;

				if (cluster.aliases.TryGetValue(alias, out node))
				{
					node.referenceCount++;
				}
				else
				{
					if (!FindAlias(friends, alias))
					{
						friends.Add(alias);
					}
				}
			}
		}

		private static bool FindAlias(List<Host> friends, Host alias)
		{
			foreach (Host host in friends)
			{
				if (host.Equals(alias))
				{
					return true;
				}
			}
			return false;
		}

		private void UpdatePartitions(Connection conn, Dictionary<string, string> infoMap)
		{
			string genString = infoMap["partition-generation"];

			if (genString == null || genString.Length == 0)
			{
				throw new AerospikeException.Parse("partition-generation is empty");
			}

			int generation = Convert.ToInt32(genString);

			if (partitionGeneration != generation)
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Node " + this + " partition generation " + generation + " changed.");
				}
				cluster.UpdatePartitions(conn, this);
				partitionGeneration = generation;
			}
		}

		/// <summary>
		/// Get a socket connection from connection pool to the server node.
		/// </summary>
		/// <param name="timeoutMillis">connection timeout value in milliseconds if a new connection is created</param>	
		/// <exception cref="AerospikeException">if a connection could not be provided</exception>
		public Connection GetConnection(int timeoutMillis)
		{
			Connection conn;
			while (connectionQueue.TryTake(out conn))
			{
				if (conn.IsValid())
				{
					try
					{
						conn.SetTimeout(timeoutMillis);
						return conn;
					}
					catch (Exception e)
					{
						// Set timeout failed. Something is probably wrong with timeout
						// value itself, so don't empty queue retrying.  Just get out.
						conn.Close();
						throw new AerospikeException.Connection(e);
					}
				}
				conn.Close();
			}
			return new Connection(address, timeoutMillis, cluster.maxSocketIdle);
		}

		/// <summary>
		/// Put connection back into connection pool.
		/// </summary>
		/// <param name="conn">socket connection</param>
		public void PutConnection(Connection conn)
		{
			if (!active || !connectionQueue.TryAdd(conn))
			{
				conn.Close();
			}
		}

		/// <summary>
		/// Set node status as healthy after successful database operation.
		/// </summary>
		public void RestoreHealth()
		{
			// There can be cases where health is full, but active is false.
			// Once a node has been marked inactive, it stays inactive.
			health = FULL_HEALTH;
		}

		/// <summary>
		/// Decrease server health status after a connection failure.
		/// </summary>
		public void DecreaseHealth()
		{
			Interlocked.Decrement(ref health);
		}

		/// <summary>
		/// Has consecutive node connection errors become critical. 
		/// </summary>
		public bool Unhealthy
		{
			get
			{
				return health <= 0;
			}
		}

		/// <summary>
		/// Return server node IP address and port.
		/// </summary>
		public Host Host
		{
			get
			{
				return host;
			}
		}

		/// <summary>
		/// Return whether node is currently active.
		/// </summary>
		public bool Active
		{
			get
			{
				return active;
			}
		}

		/// <summary>
		/// Return server node name.
		/// </summary>
		public string Name
		{
			get
			{
				return name;
			}
		}

		/// <summary>
		/// Return server node IP address aliases.
		/// </summary>
		public Host[] Aliases
		{
			get
			{
				return aliases;
			}
		}

		/// <summary>
		/// Add node alias to list.
		/// </summary>
		public void AddAlias(Host aliasToAdd)
		{
			// Aliases are only referenced in the cluster tend thread,
			// so synchronization is not necessary.
			Host[] tmpAliases = new Host[aliases.Length + 1];
			int count = 0;

			foreach (Host host in aliases)
			{
				tmpAliases[count++] = host;
			}
			tmpAliases[count] = aliasToAdd;
			aliases = tmpAliases;
		}

		public override sealed string ToString()
		{
			return name + ' ' + host;
		}

		public override sealed int GetHashCode()
		{
			return name.GetHashCode();
		}

		public override sealed bool Equals(object obj)
		{
			Node other = (Node) obj;
			return this.name.Equals(other.name);
		}

		/// <summary>
		/// Close all server node socket connections.
		/// </summary>
		public void Close()
		{
			active = false;
			CloseConnections();
			GC.SuppressFinalize(this);
		}

		protected internal virtual void CloseConnections()
		{
			// Empty connection pool.
			Connection conn;
			while (connectionQueue.TryTake(out conn))
			{
				conn.Close();
			}
		}
	}
}
