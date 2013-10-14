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
using System.Net.Sockets;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous command handler.
	/// </summary>
	public abstract class AsyncCommand
	{
		private static readonly EventHandler<SocketAsyncEventArgs> SocketListener = new EventHandler<SocketAsyncEventArgs>(SocketHandler);

		protected internal AsyncConnection conn;
		protected internal readonly AsyncCluster cluster;
		protected internal AsyncNode node;
		private DateTime limit;
		protected internal byte[] byteBuffer;
		protected internal int byteLength;
		protected internal int byteOffset;
		protected internal int timeout;
		private bool complete;

		public AsyncCommand(AsyncCluster cluster)
		{
			this.cluster = cluster;
		}

		public virtual void Execute(Policy policy, Command command)
		{
			if (policy == null)
			{
				policy = new Policy();
			}

			timeout = policy.timeout;

			if (timeout > 0)
			{
				limit = DateTime.Now.AddMilliseconds(timeout);
				AsyncTimeoutQueue.Instance.Add(this);
			}

			byteBuffer = cluster.GetByteBuffer();
			byteLength = command.sendOffset;

			if (byteLength > byteBuffer.Length)
			{
				byteBuffer = new byte[byteLength];
			}
			Array.Copy(command.sendBuffer, byteBuffer, byteLength);

			try
			{
				node = GetNode();
				conn = node.GetAsyncConnection();

				if (conn == null)
				{
					conn = new AsyncConnection(node.address, cluster, this, SocketListener);

					if (!conn.ConnectAsync())
					{
						ConnectEvent(conn.Args);
					}
				}
				else
				{
					SocketAsyncEventArgs args = conn.SetCommand(this);
					ConnectEvent(args);
				}
			}
			catch (AerospikeException.InvalidNode ain)
			{
				cluster.PutByteBuffer(byteBuffer);
				throw ain;
			}
			catch (AerospikeException.Connection ce)
			{
				// Socket connection error has occurred.
				node.DecreaseHealth();
				cluster.PutByteBuffer(byteBuffer);
				throw ce;
			}
			catch (Exception e)
			{
				if (conn != null)
				{
					node.PutAsyncConnection(conn);
				}
				cluster.PutByteBuffer(byteBuffer);
				throw new AerospikeException(e);
			}
		}

		static void SocketHandler(object sender, SocketAsyncEventArgs args)
		{
			AsyncCommand command = args.UserToken as AsyncCommand;

			if (args.SocketError != SocketError.Success)
			{
				command.FailConnection(new AerospikeException.Connection("Socket error: " + args.SocketError));
				return;
			}

			try
			{
				switch (args.LastOperation)
				{
					case SocketAsyncOperation.Receive:
						command.ReceiveEvent(args);
						break;
					case SocketAsyncOperation.Send:
						command.SendEvent(args);
						break;
					case SocketAsyncOperation.Connect:
						command.ConnectEvent(args);
						break;
					default:
						command.FailConnection(new AerospikeException.Connection("Invalid socket operation: " + args.LastOperation));
						break;
				}
			}
			catch (AerospikeException.Connection ac)
			{
				command.FailConnection(ac);
			}
			catch (AerospikeException e)
			{
				command.FailCommand(e);
			}
			catch (SocketException se)
			{
				command.FailConnection(se);
			}
			catch (Exception e)
			{
				command.FailCommand(new AerospikeException(e));
			}
		}

		private void ConnectEvent(SocketAsyncEventArgs args)
		{
			byteOffset = 0;
			args.SetBuffer(byteBuffer, byteOffset, byteLength);
			Send(args);
		}

		private void Send(SocketAsyncEventArgs args)
		{
			if (! conn.SendAsync(args))
			{
				SendEvent(args);
			}
		}

		private void SendEvent(SocketAsyncEventArgs args)
		{
			byteOffset += args.BytesTransferred;

			if (byteOffset < byteLength)
			{
				Send(args);
			}
			else
			{
				ReceiveBegin(args);
			}
		}

		public void ReceiveBegin(SocketAsyncEventArgs args)
		{
			byteOffset = 0;
			byteLength = 8;
			args.SetBuffer(byteOffset, byteLength);
			Receive(args);
		}

		public void Receive(SocketAsyncEventArgs args)
		{
			if (! conn.ReceiveAsync(args))
			{
				ReceiveEvent(args);
			}
		}

		protected internal bool CheckTimeout()
		{
			if (complete)
			{
				return false;
			}

			if (limit != null && DateTime.Now > limit)
			{
				// Command has timed out.
				/*
				if (Log.debugEnabled()) {
					int elapsed = ((int)(current - limit)) + timeout;
					Log.debug("Client timeout: timeout=" + timeout + " elapsed=" + elapsed);
				}
				*/
				node.DecreaseHealth();
				Fail(new AerospikeException.Timeout());
				return false;
			}
			return true;
		}

		protected internal void Finish()
		{
			complete = true;
			conn.UpdateLastUsed();
			node.PutAsyncConnection(conn);
			node.RestoreHealth();
			cluster.PutByteBuffer(byteBuffer);
			OnSuccess();
		}

		protected internal void FailConnection(AerospikeException ae)
		{
			if (Log.DebugEnabled())
			{
				Log.Debug("Node " + node + ": " + Util.GetErrorMessage(ae));
			}
			node.DecreaseHealth();
			Fail(ae);
		}

		protected internal void FailConnection(SocketException se)
		{
			if (Log.DebugEnabled())
			{
				Log.Debug("Node " + node + ": " + Util.GetErrorMessage(se));
			}
			// IO error means connection to server node is unhealthy.
			// Reflect this status.
			node.DecreaseHealth();
			Fail(new AerospikeException(se));
		}

		protected internal void FailCommand(AerospikeException ae)
		{
			if (Log.DebugEnabled())
			{
				Log.Debug("Node " + node + ": " + Util.GetErrorMessage(ae));
			}
			Fail(ae);
		}

		private void Fail(AerospikeException ae)
		{
			complete = true;

			if (conn != null)
			{
				conn.Close();
			}
			cluster.PutByteBuffer(byteBuffer);
			OnFailure(ae);
		}

		protected internal abstract AsyncNode GetNode();
		protected internal abstract void ReceiveEvent(SocketAsyncEventArgs args);
		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}