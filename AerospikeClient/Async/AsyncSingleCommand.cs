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
	public abstract class AsyncSingleCommand : AsyncCommand
	{
		protected internal readonly Key key;
		private readonly Partition partition;
		private bool inHeader = true;

		public AsyncSingleCommand(AsyncCluster cluster, Key key) 
			: base(cluster)
		{
			this.key = key;
			this.partition = new Partition(key);
		}

		protected internal sealed override AsyncNode GetNode()
		{
			return (AsyncNode)cluster.GetNode(partition);
		}

		protected internal sealed override void ReceiveEvent(SocketAsyncEventArgs args)
		{
			byteOffset += args.BytesTransferred;
			//Log.Info("Receive Event: " + args.BytesTransferred + "," + byteOffset + "," + byteLength + "," + inHeader);

			if (byteOffset < byteLength)
			{
				args.SetBuffer(byteOffset, byteLength - byteOffset);
				Receive(args);
				return;
			}
			byteOffset = 0;

			if (inHeader)
			{
				byteLength = (int)(ByteUtil.BytesToLong(byteBuffer, 0) & 0xFFFFFFFFFFFFL);

				if (byteLength <= 0)
				{
					Finish();
					return;
				}

				inHeader = false;

				if (byteLength > byteBuffer.Length)
				{
					byteBuffer = new byte[byteLength];
					args.SetBuffer(byteBuffer, byteOffset, byteLength);
				}
				else
				{
					args.SetBuffer(byteOffset, byteLength);
				}
				Receive(args);
			}
			else
			{
				ParseResult();
				Finish();
			}
		}

		protected internal abstract void ParseResult();
	}
}