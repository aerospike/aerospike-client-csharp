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
using System.Collections.Generic;
using System.Net.Sockets;

namespace Aerospike.Client
{
	public abstract class AsyncMultiCommand : AsyncCommand
	{
		private readonly AsyncMultiExecutor parent;
		private readonly new AsyncNode node;
		private readonly HashSet<string> binNames;
		protected internal int resultCode;
		protected internal int generation;
		protected internal int expiration;
		protected internal int fieldCount;
		protected internal int opCount;
		private readonly bool stopOnNotFound;
		private bool inHeader = true;

		public AsyncMultiCommand(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, bool stopOnNotFound) 
			: base(cluster)
		{
			this.parent = parent;
			this.node = node;
			this.stopOnNotFound = stopOnNotFound;
			this.binNames = null;
		}

		public AsyncMultiCommand(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, bool stopOnNotFound, HashSet<string> binNames) 
			: base(cluster)
		{
			this.parent = parent;
			this.node = node;
			this.stopOnNotFound = stopOnNotFound;
			this.binNames = binNames;
		}

		protected internal sealed override AsyncNode GetNode()
		{
			return node;
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
				if (ParseGroup())
				{
					Finish();
					return;
				}
				// Prepare for next group.
				inHeader = true;
				ReceiveBegin(args);
			}
		}

		private bool ParseGroup()
		{
			// Parse each message response and add it to the result array
			byteOffset = 0;

			while (byteOffset < byteLength)
			{
				resultCode = byteBuffer[byteOffset + 5];

				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
					{
						if (stopOnNotFound)
						{
							return true;
						}
					}
					else
					{
						throw new AerospikeException(resultCode);
					}
				}

				// If this is the end marker of the response, do not proceed further
				if ((byteBuffer[byteOffset + 3] & Command.INFO3_LAST) != 0)
				{
					return true;
				}
				generation = ByteUtil.BytesToInt(byteBuffer, byteOffset + 6);
				expiration = ByteUtil.BytesToInt(byteBuffer, byteOffset + 10);
				fieldCount = ByteUtil.BytesToShort(byteBuffer, byteOffset + 18);
				opCount = ByteUtil.BytesToShort(byteBuffer, byteOffset + 20);

				byteOffset += Command.MSG_REMAINING_HEADER_SIZE;

				Key key = ParseKey();
				ParseRow(key);
			}
			return false;
		}

		protected internal Key ParseKey()
		{
			byte[] digest = null;
			string ns = null;
			string setName = null;

			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(byteBuffer, byteOffset);
				byteOffset += 4;

				int fieldtype = byteBuffer[byteOffset++];
				int size = fieldlen - 1;

				if (fieldtype == FieldType.DIGEST_RIPE)
				{
					digest = new byte[size];
					Array.Copy(byteBuffer, byteOffset, digest, 0, size);
					byteOffset += size;
				}
				else if (fieldtype == FieldType.NAMESPACE)
				{
					ns = ByteUtil.Utf8ToString(byteBuffer, byteOffset, size);
					byteOffset += size;
				}
				else if (fieldtype == FieldType.TABLE)
				{
					setName = ByteUtil.Utf8ToString(byteBuffer, byteOffset, size);
					byteOffset += size;
				}
			}
			return new Key(ns, digest, setName);
		}

		protected internal virtual Record ParseRecord()
		{
			Dictionary<string, object> bins = null;

			for (int i = 0 ; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(byteBuffer, byteOffset);
				byte particleType = byteBuffer[byteOffset + 5];
				byte nameSize = byteBuffer[byteOffset + 7];
				string name = ByteUtil.Utf8ToString(byteBuffer, byteOffset + 8, nameSize);
				byteOffset += 4 + 4 + nameSize;

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				object value = ByteUtil.BytesToParticle(particleType, byteBuffer, byteOffset, particleBytesSize);
				byteOffset += particleBytesSize;

				if (bins == null)
				{
					bins = new Dictionary<string, object>();
				}
				bins[name] = value;
			}
			return new Record(bins, generation, expiration);
		}

		protected internal override void OnSuccess()
		{
			parent.ChildSuccess();
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			parent.ChildFailure(e);
		}

		protected internal abstract void ParseRow(Key key);
	}
}