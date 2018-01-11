/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestOperate : TestSync
	{
		[TestMethod]
		public void Operate()
		{
			// Write initial record.
			Key key = new Key(args.ns, args.set, "opkey");
			Bin bin1 = new Bin("optintbin", 7);
			Bin bin2 = new Bin("optstringbin", "string value");
			client.Put(null, key, bin1, bin2);

			// Add integer, write new string and read record.
			Bin bin3 = new Bin(bin1.name, 4);
			Bin bin4 = new Bin(bin2.name, "new string");
			Record record = client.Operate(null, key, Operation.Add(bin3), Operation.Put(bin4), Operation.Get());
			AssertBinEqual(key, record, bin3.name, 11);
			AssertBinEqual(key, record, bin4);
		}
	}
}
