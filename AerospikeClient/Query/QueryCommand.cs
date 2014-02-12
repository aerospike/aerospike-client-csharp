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
namespace Aerospike.Client
{
	public abstract class QueryCommand : MultiCommand
	{
		private readonly Policy policy;
		private readonly Statement statement;
		protected internal volatile bool valid = true;

		public QueryCommand(Node node, Policy policy, Statement statement)
			: base(node)
		{
			this.policy = policy;
			this.statement = statement;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			byte[] functionArgBuffer = null;
			int fieldCount = 0;
			int filterSize = 0;
			int binNameSize = 0;

			Begin();

			if (statement.ns != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.indexName != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.indexName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.setName != null)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.filters != null)
			{
				dataOffset += FIELD_HEADER_SIZE;
				filterSize++; // num filters

				foreach (Filter filter in statement.filters)
				{
					filterSize += filter.EstimateSize();
				}
				dataOffset += filterSize;
				fieldCount++;
			}
			else
			{
				// Calling query with no filters is more efficiently handled by a primary index scan. 
				// Estimate scan options size.
				dataOffset += 2 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.binNames != null)
			{
				dataOffset += FIELD_HEADER_SIZE;
				binNameSize++; // num bin names

				foreach (string binName in statement.binNames)
				{
					binNameSize += ByteUtil.EstimateSizeUtf8(binName) + 1;
				}
				dataOffset += binNameSize;
				fieldCount++;
			}

			if (statement.taskId > 0)
			{
				dataOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.functionName != null)
			{
				dataOffset += FIELD_HEADER_SIZE + 1; // udf type
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.packageName) + FIELD_HEADER_SIZE;
				dataOffset += ByteUtil.EstimateSizeUtf8(statement.functionName) + FIELD_HEADER_SIZE;

				if (statement.functionArgs.Length > 0)
				{
					functionArgBuffer = MsgPacker.Pack(statement.functionArgs);
				}
				else
				{
					functionArgBuffer = new byte[0];
				}
				dataOffset += FIELD_HEADER_SIZE + functionArgBuffer.Length;
				fieldCount += 4;
			}

			SizeBuffer();
			byte readAttr = (byte)Command.INFO1_READ;
			WriteHeader(readAttr, 0, fieldCount, 0);

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
				dataBuffer[dataOffset++] = (byte)statement.filters.Length;

				foreach (Filter filter in statement.filters)
				{
					dataOffset = filter.Write(dataBuffer, dataOffset);
				}
			}
			else
			{
				// Calling query with no filters is more efficiently handled by a primary index scan. 
				WriteFieldHeader(2, FieldType.SCAN_OPTIONS);
				byte priority = (byte)policy.priority;
				priority <<= 4;
				dataBuffer[dataOffset++] = priority;
				dataBuffer[dataOffset++] = (byte)100;
			}

			if (statement.binNames != null)
			{
				WriteFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
				dataBuffer[dataOffset++] = (byte)statement.binNames.Length;

				foreach (string binName in statement.binNames)
				{
					int len = ByteUtil.StringToUtf8(binName, dataBuffer, dataOffset + 1);
					dataBuffer[dataOffset] = (byte)len;
					dataOffset += len + 1;
				}
			}

			if (statement.taskId > 0)
			{
				WriteFieldHeader(8, FieldType.TRAN_ID);
				ByteUtil.LongToBytes((ulong)statement.taskId, dataBuffer, dataOffset);
				dataOffset += 8;
			}

			if (statement.functionName != null)
			{
				WriteFieldHeader(1, FieldType.UDF_OP);
				dataBuffer[dataOffset++] = (statement.returnData) ? (byte)1 : (byte)2;
				WriteField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
				WriteField(statement.functionName, FieldType.UDF_FUNCTION);
				WriteField(functionArgBuffer, FieldType.UDF_ARGLIST);
			}
			End();
		}

		public void Stop()
		{
			valid = false;
		}
	}
}
