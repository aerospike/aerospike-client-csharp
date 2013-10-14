/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
namespace Aerospike.Client
{
	public sealed class DeleteCommand : SingleCommand
	{
		private int resultCode;

		public DeleteCommand(Cluster cluster, Key key) : base(cluster, key)
		{
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.		
			conn.ReadFully(receiveBuffer, MSG_TOTAL_HEADER_SIZE);

			long sz = ByteUtil.BytesToLong(receiveBuffer, 0);
			byte headerLength = receiveBuffer[8];
			resultCode = receiveBuffer[13];
			int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;

			// Read remaining message bytes.
			if (receiveSize > 0)
			{
				ResizeReceiveBuffer(receiveSize);
				conn.ReadFully(receiveBuffer, receiveSize);
			}

			if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR)
			{
				throw new AerospikeException(resultCode);
			}
		}

		public int GetResultCode()
		{
			return resultCode;
		}
	}
}