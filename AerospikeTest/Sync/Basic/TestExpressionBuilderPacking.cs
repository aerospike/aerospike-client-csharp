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
using System.Collections;

namespace Aerospike.Test
{
	/// <summary>
	/// Packing-only regression tests for expression builders. These do not contact
	/// the server and do not validate expression semantics — see TestListExp and
	/// TestMapExp for server-evaluated behavior.
	/// </summary>
	[TestClass]
	public class TestExpressionBuilderPacking
	{
		[TestMethod]
		public void ListAndMapExpBuildersPackWithoutError()
		{
			Exp listBin = Exp.ListBin("listbin");
			Exp mapBin = Exp.MapBin("mapbin");
			Exp value = Exp.Val(1);
			Exp values = Exp.Val((IList)new List<object> { 1, 2 });
			ListPolicy listPolicy = ListPolicy.Default;
			MapPolicy mapPolicy = new(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
			CTX listCtx = CTX.ListIndex(0);
			CTX mapCtx = CTX.MapKey(Value.Get("nested"));

			AssertExpBytes(ListExp.Append(listPolicy, value, listBin));
			AssertExpBytes(ListExp.AppendItems(listPolicy, values, listBin));
			AssertExpBytes(ListExp.Insert(listPolicy, Exp.Val(0), value, listBin));
			AssertExpBytes(ListExp.InsertItems(listPolicy, Exp.Val(0), values, listBin));
			AssertExpBytes(ListExp.Increment(listPolicy, Exp.Val(0), value, listBin));
			AssertExpBytes(ListExp.Set(listPolicy, Exp.Val(0), value, listBin));
			AssertExpBytes(ListExp.Clear(listBin));
			AssertExpBytes(ListExp.Sort(ListSortFlags.DEFAULT, listBin));
			AssertExpBytes(ListExp.RemoveByValue(ListReturnType.NONE, value, listBin));
			AssertExpBytes(ListExp.RemoveByValueList(ListReturnType.INVERTED, values, listBin));
			AssertExpBytes(ListExp.RemoveByValueRange(ListReturnType.NONE, Exp.Val(0), Exp.Val(10), listBin));
			AssertExpBytes(ListExp.RemoveByValueRelativeRankRange(ListReturnType.NONE, value, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.RemoveByValueRelativeRankRange(ListReturnType.NONE, value, Exp.Val(0), Exp.Val(1), listBin));
			AssertExpBytes(ListExp.RemoveByIndex(Exp.Val(0), listBin));
			AssertExpBytes(ListExp.RemoveByIndexRange(ListReturnType.NONE, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.RemoveByIndexRange(ListReturnType.NONE, Exp.Val(0), Exp.Val(1), listBin));
			AssertExpBytes(ListExp.RemoveByRank(Exp.Val(0), listBin));
			AssertExpBytes(ListExp.RemoveByRankRange(ListReturnType.NONE, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.RemoveByRankRange(ListReturnType.NONE, Exp.Val(0), Exp.Val(1), listBin));
			AssertExpBytes(ListExp.Size(listBin));
			AssertExpBytes(ListExp.Join(listBin));
			AssertExpBytes(ListExp.Join(Exp.Val(","), listBin));
			AssertExpBytes(ListExp.GetByValue(ListReturnType.COUNT, value, listBin));
			AssertExpBytes(ListExp.GetByValueRange(ListReturnType.VALUE, Exp.Val(0), Exp.Val(10), listBin));
			AssertExpBytes(ListExp.GetByValueList(ListReturnType.COUNT, values, listBin));
			AssertExpBytes(ListExp.GetByValueRelativeRankRange(ListReturnType.VALUE, value, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.GetByIndexRange(ListReturnType.VALUE, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.GetByRank(ListReturnType.VALUE, Exp.Type.INT, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.GetByRankRange(ListReturnType.VALUE, Exp.Val(0), listBin));
			AssertExpBytes(ListExp.Append(listPolicy, value, listBin, listCtx));

			Exp mapKey = Exp.Val("key");
			AssertExpBytes(MapExp.Put(mapPolicy, mapKey, value, mapBin));
			AssertExpBytes(MapExp.PutItems(mapPolicy, values, mapBin));
			AssertExpBytes(MapExp.Increment(mapPolicy, mapKey, value, mapBin));
			AssertExpBytes(MapExp.Clear(mapBin));
			AssertExpBytes(MapExp.RemoveByKey(mapKey, mapBin));
			AssertExpBytes(MapExp.RemoveByKeyList(MapReturnType.INVERTED, values, mapBin));
			AssertExpBytes(MapExp.RemoveByKeyRange(MapReturnType.NONE, Exp.Val("a"), Exp.Val("z"), mapBin));
			AssertExpBytes(MapExp.RemoveByKeyRelativeIndexRange(MapReturnType.NONE, mapKey, Exp.Val(0), mapBin));
			AssertExpBytes(MapExp.RemoveByValue(MapReturnType.NONE, value, mapBin));
			AssertExpBytes(MapExp.RemoveByValueList(MapReturnType.INVERTED, values, mapBin));
			AssertExpBytes(MapExp.RemoveByValueRange(MapReturnType.NONE, Exp.Val(0), Exp.Val(10), mapBin));
			AssertExpBytes(MapExp.RemoveByValueRelativeRankRange(MapReturnType.NONE, value, Exp.Val(0), mapBin));
			AssertExpBytes(MapExp.RemoveByIndex(Exp.Val(0), mapBin));
			AssertExpBytes(MapExp.RemoveByRank(Exp.Val(0), mapBin));
			AssertExpBytes(MapExp.Size(mapBin));
			AssertExpBytes(MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.INT, mapKey, mapBin));
			AssertExpBytes(MapExp.GetByKeyRange(MapReturnType.COUNT, Exp.Val("a"), Exp.Val("z"), mapBin));
			AssertExpBytes(MapExp.GetByKeyList(MapReturnType.COUNT, values, mapBin));
			AssertExpBytes(MapExp.GetByValue(MapReturnType.COUNT, value, mapBin));
			AssertExpBytes(MapExp.GetByValueRange(MapReturnType.VALUE, Exp.Val(0), Exp.Val(10), mapBin));
			AssertExpBytes(MapExp.GetByIndex(MapReturnType.VALUE, Exp.Type.INT, Exp.Val(0), mapBin));
			AssertExpBytes(MapExp.GetByRank(MapReturnType.VALUE, Exp.Type.INT, Exp.Val(0), mapBin));
			AssertExpBytes(MapExp.Put(mapPolicy, mapKey, value, mapBin, mapCtx));
		}

		private static void AssertExpBytes(Exp exp)
		{
			byte[] bytes = Exp.Build(exp).Bytes;
			Assert.IsNotNull(bytes);
			Assert.IsTrue(bytes.Length > 0);
		}
	}
}
