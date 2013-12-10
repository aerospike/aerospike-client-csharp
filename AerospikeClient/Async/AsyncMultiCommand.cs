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
		private readonly AsyncNode fixedNode;
		protected internal int resultCode;
		protected internal int generation;
		protected internal int expiration;
		protected internal int fieldCount;
		protected internal int opCount;
		private readonly bool stopOnNotFound;

		public AsyncMultiCommand(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, bool stopOnNotFound) 
			: base(cluster)
		{
			this.parent = parent;
			this.fixedNode = node;
			this.stopOnNotFound = stopOnNotFound;
		}

		protected internal sealed override AsyncNode GetNode()
		{
			return fixedNode;
		}

		protected internal sealed override void ParseCommand()
		{
			if (ParseGroup())
			{
				Finish();
				return;
			}
			// Prepare for next group.
			inHeader = true;
			ReceiveBegin();
		}

		private bool ParseGroup()
		{
			// Parse each message response and add it to the result array
			dataOffset = 0;

			while (dataOffset < dataLength)
			{
				resultCode = dataBuffer[dataOffset + 5];

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
				if ((dataBuffer[dataOffset + 3] & Command.INFO3_LAST) != 0)
				{
					return true;
				}
				generation = ByteUtil.BytesToInt(dataBuffer, dataOffset + 6);
				expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset + 10);
				fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 18);
				opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 20);

				dataOffset += Command.MSG_REMAINING_HEADER_SIZE;

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
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int fieldtype = dataBuffer[dataOffset++];
				int size = fieldlen - 1;

				if (fieldtype == FieldType.DIGEST_RIPE)
				{
					digest = new byte[size];
					Array.Copy(dataBuffer, dataOffset, digest, 0, size);
					dataOffset += size;
				}
				else if (fieldtype == FieldType.NAMESPACE)
				{
					ns = ByteUtil.Utf8ToString(dataBuffer, dataOffset, size);
					dataOffset += size;
				}
				else if (fieldtype == FieldType.TABLE)
				{
					setName = ByteUtil.Utf8ToString(dataBuffer, dataOffset, size);
					dataOffset += size;
				}
			}
			return new Key(ns, digest, setName);
		}

		protected internal virtual Record ParseRecord()
		{
			Dictionary<string, object> bins = null;

			for (int i = 0 ; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				byte particleType = dataBuffer[dataOffset + 5];
				byte nameSize = dataBuffer[dataOffset + 7];
				string name = ByteUtil.Utf8ToString(dataBuffer, dataOffset + 8, nameSize);
				dataOffset += 4 + 4 + nameSize;

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				object value = ByteUtil.BytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
				dataOffset += particleBytesSize;

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