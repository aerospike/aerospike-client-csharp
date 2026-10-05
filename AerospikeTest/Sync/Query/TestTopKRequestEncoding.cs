/*
 * Copyright 2012-2026 Aerospike, Inc.
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
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Verifies Top-K order-by / limit fields are encoded on the wire only when pushdown is enabled.
	/// </summary>
	[TestClass]
	public class TestTopKRequestEncoding : TestSync
	{
		[TestMethod]
		public void TopKFieldsAreEncodedOnlyForPushdown()
		{
			Cluster cluster = ((AerospikeClient)client).Cluster;
			Node node = cluster.GetRandomNode();
			Statement statement = MakeStatement();

			Dictionary<int, byte[]> pushed = Fields(Build(cluster, node, statement, true));
			Assert.AreEqual(6, pushed.Count);
			CollectionAssert.AreEqual(new byte[] { 1, 1, 0, 4, (byte)'r', (byte)'a', (byte)'n', (byte)'k' }, pushed[FieldType.ORDER_BY]);
			CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 3 }, pushed[FieldType.TOP_K]);

			Dictionary<int, byte[]> fallback = Fields(Build(cluster, node, statement, false));
			Assert.AreEqual(4, fallback.Count);
			Assert.IsFalse(fallback.ContainsKey(FieldType.ORDER_BY));
			Assert.IsFalse(fallback.ContainsKey(FieldType.TOP_K));
		}

		private static Statement MakeStatement()
		{
			Statement statement = new();
			statement.SetNamespace(SuiteHelpers.ns);
			statement.SetSetName("topKWire");
			statement.SetBinNames("rank");
			statement.SetOrderBy("rank", BinDataType.INTEGER, Order.DESC);
			statement.SetTopK(3);
			return statement;
		}

		private static byte[] Build(Cluster cluster, Node node, Statement statement, bool sendTopK)
		{
			TestCommand command = new();
			command.BuildQuery(cluster, new QueryPolicy(), statement, 1, false, null, node, sendTopK);
			byte[] copy = new byte[command.DataOffset];
			Array.Copy(command.DataBuffer, 0, copy, 0, command.DataOffset);
			return copy;
		}

		private static Dictionary<int, byte[]> Fields(byte[] buffer)
		{
			int fieldCount = ((buffer[26] & 0xff) << 8) | (buffer[27] & 0xff);
			Dictionary<int, byte[]> fields = new();
			int offset = Command.MSG_TOTAL_HEADER_SIZE;

			for (int i = 0; i < fieldCount; i++)
			{
				int size = ByteUtil.BytesToInt(buffer, offset);
				int type = buffer[offset + 4] & 0xff;
				byte[] value = new byte[size - 1];
				Array.Copy(buffer, offset + 5, value, 0, size - 1);
				fields[type] = value;
				offset += 4 + size;
			}

			Assert.AreEqual(buffer.Length, offset + Command.OPERATION_HEADER_SIZE + 4);
			Assert.IsTrue(fields.ContainsKey(FieldType.NAMESPACE));
			Assert.IsTrue(fields.ContainsKey(FieldType.TABLE));
			return fields;
		}

		private sealed class TestCommand : Command
		{
			internal TestCommand() : base(0, 0, 0)
			{
			}

			internal byte[] DataBuffer => dataBuffer;
			internal int DataOffset => dataOffset;

			internal void BuildQuery(
				Cluster cluster,
				QueryPolicy policy,
				Statement statement,
				ulong taskId,
				bool background,
				NodePartitions nodePartitions,
				Node node,
				bool sendTopK)
			{
				SetQuery(cluster, policy, statement, taskId, background, nodePartitions, node, sendTopK);
			}

			protected override int SizeBuffer()
			{
				dataBuffer = new byte[dataOffset];
				dataOffset = 0;
				return dataBuffer.Length;
			}

			protected override void End()
			{
			}

			protected override void SetLength(int length)
			{
			}
		}
	}
}
