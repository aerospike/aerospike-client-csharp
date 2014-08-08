/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
namespace Aerospike.Client
{
	public abstract class QueryCommand : MultiCommand
	{
		private readonly Policy policy;
		private readonly Statement statement;

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
					functionArgBuffer = Packer.Pack(statement.functionArgs);
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
	}
}
