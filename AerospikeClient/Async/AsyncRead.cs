/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncRead : AsyncSingleCommand
	{
		private readonly RecordListener listener;
		private Record record;

		public AsyncRead(AsyncCluster cluster, Key key, RecordListener listener) 
			: base(cluster, key)
		{
			this.listener = listener;
		}

		protected internal override void ParseResult()
		{
			int resultCode = byteBuffer[5];
			int generation = ByteUtil.BytesToInt(byteBuffer, 6);
			int expiration = ByteUtil.BytesToInt(byteBuffer, 10);
			int fieldCount = ByteUtil.BytesToShort(byteBuffer, 18);
			int opCount = ByteUtil.BytesToShort(byteBuffer, 20);
			byteOffset = Command.MSG_REMAINING_HEADER_SIZE;

			if (resultCode == 0)
			{
				if (opCount == 0)
				{
					// Bin data was not returned.
					record = new Record(null, generation, expiration);
				}
				else
				{
					record = ParseRecord(opCount, fieldCount, generation, expiration);
				}
			}
			else
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					record = null;
				}
				else
				{
					throw new AerospikeException(resultCode);
				}
			}
		}

		private Record ParseRecord(int opCount, int fieldCount, int generation, int expiration)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			if (fieldCount > 0)
			{
				// Just skip over all the fields
				for (int i = 0; i < fieldCount; i++)
				{
					int fieldSize = ByteUtil.BytesToInt(byteBuffer, byteOffset);
					byteOffset += 4 + fieldSize;
				}
			}

			Dictionary<string, object> bins = null;

			for (int i = 0; i < opCount; i++)
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
			if (listener != null)
			{
				listener.OnSuccess(key, record);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}