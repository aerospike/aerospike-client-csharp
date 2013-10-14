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
	public abstract class QueryCommand : MultiCommand
	{
		protected internal volatile bool valid = true;

		public QueryCommand(Node node) : base(node)
		{
		}

		public virtual void Query(Policy policy, Statement statement)
		{
			byte[] functionArgBuffer = null;
			int fieldCount = 0;
			int filterSize = 0;
			int binNameSize = 0;

			if (statement.ns != null)
			{
				sendOffset += ByteUtil.EstimateSizeUtf8(statement.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.indexName != null)
			{
				sendOffset += ByteUtil.EstimateSizeUtf8(statement.indexName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.setName != null)
			{
				sendOffset += ByteUtil.EstimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.filters != null)
			{
				sendOffset += FIELD_HEADER_SIZE;
				filterSize++; // num filters

				foreach (Filter filter in statement.filters)
				{
					filterSize += filter.EstimateSize();
				}
				sendOffset += filterSize;
				fieldCount++;
			}
			else
			{
				// Calling query with no filters is more efficiently handled by a primary index scan. 
				// Estimate scan options size.
				sendOffset += 2 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.binNames != null)
			{
				sendOffset += FIELD_HEADER_SIZE;
				binNameSize++; // num bin names

				foreach (string binName in statement.binNames)
				{
					binNameSize += ByteUtil.EstimateSizeUtf8(binName) + 1;
				}
				sendOffset += binNameSize;
				fieldCount++;
			}

			if (statement.taskId > 0)
			{
				sendOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.functionName != null)
			{
				sendOffset += FIELD_HEADER_SIZE + 1; // udf type
				sendOffset += ByteUtil.EstimateSizeUtf8(statement.packageName) + FIELD_HEADER_SIZE;
				sendOffset += ByteUtil.EstimateSizeUtf8(statement.functionName) + FIELD_HEADER_SIZE;

				if (statement.functionArgs.Length > 0)
				{
					functionArgBuffer = MsgPacker.Pack(statement.functionArgs);
				}
				else
				{
					functionArgBuffer = new byte[0];
				}
				sendOffset += FIELD_HEADER_SIZE + functionArgBuffer.Length;
				fieldCount += 4;
			}

			Begin();
			byte readAttr = (byte)Command.INFO1_READ;
			WriteHeader(readAttr, fieldCount, 0);

			if (statement.ns != null)
			{
				WriteField(statement.ns, FieldType.NAMESPACE);
			}

			if (statement.indexName != null)
			{
				WriteField(statement.indexName, FieldType.INDEX_NAME);
			}

			if (statement.setName != null)
			{
				WriteField(statement.setName, FieldType.TABLE);
			}

			if (statement.filters != null)
			{
				WriteFieldHeader(filterSize, FieldType.INDEX_RANGE);
				sendBuffer[sendOffset++] = (byte)statement.filters.Length;

				foreach (Filter filter in statement.filters)
				{
					sendOffset = filter.Write(sendBuffer, sendOffset);
				}
			}
			else
			{
				// Calling query with no filters is more efficiently handled by a primary index scan. 
				WriteFieldHeader(2, FieldType.SCAN_OPTIONS);
				byte priority = (byte)policy.priority;
				priority <<= 4;
				sendBuffer[sendOffset++] = priority;
				sendBuffer[sendOffset++] = (byte)100;
			}

			if (statement.binNames != null)
			{
				WriteFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
				sendBuffer[sendOffset++] = (byte)statement.binNames.Length;

				foreach (string binName in statement.binNames)
				{
					int len = ByteUtil.StringToUtf8(binName, sendBuffer, sendOffset + 1);
					sendBuffer[sendOffset] = (byte)len;
					sendOffset += len + 1;
				}
			}

			if (statement.taskId > 0)
			{
				WriteFieldHeader(8, FieldType.TRAN_ID);
				ByteUtil.LongToBytes((ulong)statement.taskId, sendBuffer, sendOffset);
				sendOffset += 8;
			}

			if (statement.functionName != null)
			{
				WriteFieldHeader(1, FieldType.UDF_OP);
				sendBuffer[sendOffset++] = (statement.returnData) ? (byte)1 : (byte)2;
				WriteField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
				WriteField(statement.functionName, FieldType.UDF_FUNCTION);
				WriteField(functionArgBuffer, FieldType.UDF_ARGLIST);
			}
			End();
			Execute(policy);
		}

		public void Stop()
		{
			valid = false;
		}
	}
}