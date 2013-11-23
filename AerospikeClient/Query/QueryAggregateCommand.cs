/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System.Threading;
using System.Collections.Concurrent;
using LuaInterface;

namespace Aerospike.Client
{
	public sealed class QueryAggregateCommand : QueryCommand
	{
		private readonly BlockingCollection<object> inputQueue;

		public QueryAggregateCommand(Node node, Policy policy, Statement statement, BlockingCollection<object> inputQueue)
			: base(node, policy, statement)
		{
			this.inputQueue = inputQueue;
		}

		protected internal override bool ParseRecordResults(int receiveSize)
		{
			// Read/parse remaining message bytes one record at a time.
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

				if (opCount != 1)
				{
					throw new AerospikeException("Query aggregate expected exactly one bin.  Received " + opCount);
				}

				// Parse aggregateValue.
				ReadBytes(8);
				int opSize = ByteUtil.BytesToInt(dataBuffer, 0);
				byte particleType = dataBuffer[5];
				byte nameSize = dataBuffer[7];

				ReadBytes(nameSize);
				string name = ByteUtil.Utf8ToString(dataBuffer, 0, nameSize);

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				ReadBytes(particleBytesSize);
				object aggregateValue = LuaInstance.BytesToLua(particleType, dataBuffer, 0, particleBytesSize);

				if (!name.Equals("SUCCESS"))
				{
					throw new AerospikeException("Query aggregate expected bin name SUCCESS.  Received " + name);
				}

				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}

				if (aggregateValue != null)
				{
					try
					{
						inputQueue.Add(aggregateValue);
					}
					catch (ThreadInterruptedException)
					{
					}
				}
			}
			return true;
		}
	}
}