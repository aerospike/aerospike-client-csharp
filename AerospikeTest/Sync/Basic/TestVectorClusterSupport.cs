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
	/// Vector cluster-support / fail-fast unit tests (no live cluster required).
	/// </summary>
	[TestClass]
	public class TestVectorClusterSupport
	{
		private static readonly Key Key = new("test", "set", "veckey");
		private static readonly Vector Vec = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });
		private const string ExpectedMessage = "Vector is not supported by all nodes in the cluster";

		private sealed class TestCommand : Command
		{
			public TestCommand(bool vectorSupported)
				: base(0, 0, 0)
			{
				Internals.SetVectorSupported(this, vectorSupported);
			}

			protected override int SizeBuffer()
			{
				if (dataBuffer == null || dataBuffer.Length < dataOffset)
				{
					dataBuffer = new byte[Math.Max(8192, dataOffset)];
				}
				return dataBuffer.Length;
			}

			protected override void End()
			{
			}

			protected override void SetLength(int length)
			{
			}
		}

		private static TestCommand NewCommand(bool vectorSupported) => new(vectorSupported);

		private static Expression VectorFilter()
		{
			return Exp.Build(
				Exp.GT(
					VectorExp.Distance(VectorDistanceMetric.COSINE, Vec, Exp.VectorBin("v")),
					Exp.Val(0.5)));
		}

		//-------------------------------------------------------
		// Value.hasVector detection
		//-------------------------------------------------------

		[TestMethod]
		public void ValueVectorHasVector()
		{
			Assert.IsTrue(Internals.HasVector(Value.Get(Vec)));
		}

		[TestMethod]
		public void ValueScalarHasNoVector()
		{
			Assert.IsFalse(Internals.HasVector(Value.Get(123L)));
			Assert.IsFalse(Internals.HasVector(Value.Get("abc")));
		}

		[TestMethod]
		public void ValueListNestedVector()
		{
			Assert.IsTrue(Internals.HasVector(Value.Get((object)new List<object> { Vec })));
			Assert.IsFalse(Internals.HasVector(Value.Get((object)new List<object> { "abc" })));
		}

		[TestMethod]
		public void ValueMapNestedVector()
		{
			Assert.IsTrue(Internals.HasVector(Value.Get((object)new Dictionary<object, object> { { "k", Vec } })));
			Assert.IsFalse(Internals.HasVector(Value.Get((object)new Dictionary<object, object> { { "k", "v" } })));
		}

		[TestMethod]
		public void ValueDetectsAddedVector()
		{
			List<object> list = new();
			Value value = Value.Get((object)list);

			Assert.IsFalse(Internals.HasVector(value));
			list.Add(Vec);
			Assert.IsTrue(Internals.HasVector(value));
			list.Clear();
			Assert.IsTrue(Internals.HasVector(value));
		}

		//-------------------------------------------------------
		// Expression.hasVector detection
		//-------------------------------------------------------

		[TestMethod]
		public void ExpressionWithVector()
		{
			Assert.IsTrue(Internals.HasVector(VectorFilter()));
		}

		[TestMethod]
		public void ExpressionWithoutVector()
		{
			Assert.IsFalse(Internals.HasVector(Exp.Build(Exp.EQ(Exp.IntBin("a"), Exp.Val(1)))));
		}

		[TestMethod]
		public void ExpressionFromBytesReportsNoVector()
		{
			Assert.IsFalse(Internals.HasVector(Expression.FromBytes(new byte[] { 1, 2, 3 })));
		}

		//-------------------------------------------------------
		// Operation vector detection
		//-------------------------------------------------------

		[TestMethod]
		public void OperationBinVector()
		{
			Assert.IsTrue(Internals.HasVector(Operation.Put(new Bin("v", Vec)).value));
			Assert.IsFalse(Internals.HasVector(Operation.Put(new Bin("v", 1)).value));
			Assert.IsFalse(Internals.HasVector(Operation.Get("v").value));
		}

		[TestMethod]
		public void ListOperationVector()
		{
			Assert.IsTrue(Internals.HasVector(ListOperation.Append("v", Value.Get(Vec)).value));
			Assert.IsFalse(Internals.HasVector(ListOperation.Append("v", Value.Get(1)).value));
			Assert.IsTrue(Internals.HasVector(ListOperation.AppendItems("v", new List<object> { Value.Get(Vec) }).value));
			Assert.IsTrue(Internals.HasVector(ListOperation.InsertItems("v", 0, new List<object> { Value.Get(Vec) }).value));
		}

		[TestMethod]
		public void MapOperationVector()
		{
			Assert.IsTrue(Internals.HasVector(MapOperation.Put(MapPolicy.Default, "m", Value.Get("k"), Value.Get(Vec)).value));
			Assert.IsFalse(Internals.HasVector(MapOperation.Put(MapPolicy.Default, "m", Value.Get("k"), Value.Get(1)).value));

			Dictionary<object, object> items = new() { { Value.Get("k"), Value.Get(Vec) } };
			Assert.IsTrue(Internals.HasVector(MapOperation.PutItems(MapPolicy.Default, "m", items).value));
		}

		[TestMethod]
		public void HllOperationVector()
		{
			Assert.IsTrue(Internals.HasVector(HLLOperation.Add(HLLPolicy.Default, "h", new List<object> { Value.Get(Vec) }).value));
		}

		[TestMethod]
		public void ExpOperationVector()
		{
			Assert.IsTrue(Internals.HasVector(ExpOperation.Write("v", VectorFilter(), ExpWriteFlags.DEFAULT).value));
			Assert.IsFalse(Internals.HasVector(ExpOperation.Write("v", Exp.Build(Exp.Val(1)), ExpWriteFlags.DEFAULT).value));
		}

		//-------------------------------------------------------
		// Command.checkVectorSupport guard
		//-------------------------------------------------------

		[TestMethod]
		public void CheckVectorSupportThrowsWhenUnsupported()
		{
			AerospikeException e = Assert.Throws<AerospikeException>(
				() => Internals.CheckVectorSupport(NewCommand(false), true));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains(ExpectedMessage));
		}

		[TestMethod]
		public void CheckVectorSupportAllowsWhenSupported()
		{
			Internals.CheckVectorSupport(NewCommand(false), false);
			Internals.CheckVectorSupport(NewCommand(true), true);
		}

		//-------------------------------------------------------
		// End-to-end serialization: write (bin) path
		//-------------------------------------------------------

		[TestMethod]
		public void WriteVectorBinFailsWhenUnsupported()
		{
			AssertWriteThrows(new Bin("v", Vec));
		}

		[TestMethod]
		public void WriteVectorInListBinFailsWhenUnsupported()
		{
			AssertWriteThrows(new Bin("v", new List<object> { Vec }));
		}

		[TestMethod]
		public void WriteVectorInMapBinFailsWhenUnsupported()
		{
			AssertWriteThrows(new Bin("v", new Dictionary<object, object> { { "k", Vec } }));
		}

		[TestMethod]
		public void WriteScalarBinSucceedsWhenUnsupported()
		{
			NewCommand(false).SetWrite(new WritePolicy(), Operation.Type.WRITE, Key, new Bin[] { new Bin("s", 1) });
		}

		[TestMethod]
		public void WriteVectorBinSucceedsWhenSupported()
		{
			NewCommand(true).SetWrite(new WritePolicy(), Operation.Type.WRITE, Key, new Bin[] { new Bin("v", Vec) });
		}

		[TestMethod]
		public void FilterExpVectorFailsWhenUnsupported()
		{
			WritePolicy wp = new() { filterExp = VectorFilter() };

			AerospikeException e = Assert.Throws<AerospikeException>(
				() => NewCommand(false).SetWrite(wp, Operation.Type.WRITE, Key, new Bin[] { new Bin("s", 1) }));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains(ExpectedMessage));
		}

		[TestMethod]
		public void OperateVectorCdtFailsWhenUnsupported()
		{
			Operation[] ops = new Operation[] { ListOperation.Append("v", Value.Get(Vec)) };
			WritePolicy wp = new();
			OperateArgs args = new(wp, null, null, ops);

			AerospikeException e = Assert.Throws<AerospikeException>(
				() => NewCommand(false).SetOperate(args.writePolicy, Key, args));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains(ExpectedMessage));
		}

		[TestMethod]
		public void OperateVectorPutFailsWhenUnsupported()
		{
			Operation[] ops = new Operation[] { Operation.Put(new Bin("v", Vec)) };
			WritePolicy wp = new();
			OperateArgs args = new(wp, null, null, ops);

			AerospikeException e = Assert.Throws<AerospikeException>(
				() => NewCommand(false).SetOperate(args.writePolicy, Key, args));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains(ExpectedMessage));
		}

		private static void AssertWriteThrows(Bin bin)
		{
			AerospikeException e = Assert.Throws<AerospikeException>(
				() => NewCommand(false).SetWrite(new WritePolicy(), Operation.Type.WRITE, Key, new Bin[] { bin }));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains(ExpectedMessage));
		}
	}
}
