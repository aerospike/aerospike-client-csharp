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
	public sealed class ServerCommand : QueryCommand
	{
		public ServerCommand(Node node, Policy policy, Statement statement) 
			: base(node, policy, statement)
		{
		}

		protected internal override bool ParseRecordResults(int receiveSize)
		{
			// Server commands (Query/Execute UDF) should only send back a return code.
			// Keep parsing logic to empty socket buffer just in case server does
			// send records back.
			dataOffset = 0;

			while (dataOffset < receiveSize)
			{
				ReadBytes(MSG_REMAINING_HEADER_SIZE);
				int resultCode = dataBuffer[5];

				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
					{
						return false;
					}
					throw new AerospikeException(resultCode);
				}

				byte info3 = dataBuffer[3];

				// If this is the end marker of the response, do not proceed further
				if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST)
				{
					return false;
				}

				int fieldCount = ByteUtil.BytesToShort(dataBuffer, 18);
				int opCount = ByteUtil.BytesToShort(dataBuffer, 20);

				ParseKey(fieldCount);

				for (int i = 0 ; i < opCount; i++)
				{
					ReadBytes(8);
					int opSize = ByteUtil.BytesToInt(dataBuffer, 0);
					byte nameSize = dataBuffer[7];

					ReadBytes(nameSize);

					int particleBytesSize = (int)(opSize - (4 + nameSize));
					ReadBytes(particleBytesSize);
				}

				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}
			}
			return true;
		}
	}
}