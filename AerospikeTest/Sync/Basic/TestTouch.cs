﻿/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestTouch : TestSync
	{
		[TestMethod]
		public void TouchOperate()
		{
			Key key = new Key(args.ns, args.set, "TouchOperate");
			Bin bin = new Bin(args.GetBinName("touchbin"), "touchvalue");

			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = 1;
			
			client.Put(writePolicy, key, bin);

			writePolicy.expiration = 2;
			
			Record record = client.Operate(writePolicy, key, Operation.Touch(), Operation.GetHeader());
			AssertRecordFound(key, record);
			Assert.AreNotEqual(0, record.expiration);

			Util.Sleep(1000);

			record = client.Get(null, key, bin.name);
			AssertRecordFound(key, record);

			Util.Sleep(3000);

			record = client.Get(null, key, bin.name);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void Touch()
		{
			Key key = new Key(args.ns, args.set, "touch");
			Bin bin = new Bin(args.GetBinName("touchbin"), "touchvalue");

			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = 1;

			client.Put(writePolicy, key, bin);

			writePolicy.expiration = 2;
			client.Touch(writePolicy, key);

			Util.Sleep(1000);

			var record = client.GetHeader(writePolicy, key);
			AssertRecordFound(key, record);
			Assert.AreNotEqual(0, record.expiration);

			Util.Sleep(3000);

			record = client.GetHeader(null, key);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void Touched()
		{
			Key key = new(args.ns, args.set, "touched");

			client.Delete(null, key);

			WritePolicy writePolicy = new();
			writePolicy.expiration = 10;

			bool touched = client.Touched(writePolicy, key);
			Assert.IsFalse(touched);

			Bin bin = new("touchBin", "touchValue");
			client.Put(writePolicy, key, bin);

			touched = client.Touched(writePolicy, key);
			Assert.IsTrue(touched);
		}
	}
}
