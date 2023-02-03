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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAdd : TestSync
	{
		[TestMethod]
		public void Add()
		{
			Key key = new Key(args.ns, args.set, "addkey");
			string binName = args.GetBinName("addbin");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Perform some adds and check results.
			Bin bin = new Bin(binName, 10);
			client.Add(null, key, bin);

			bin = new Bin(binName, 5);
			client.Add(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin.name, 15);

			// Test add and get combined.
			bin = new Bin(binName, 30);
			record = client.Operate(null, key, Operation.Add(bin), Operation.Get(bin.name));
			AssertBinEqual(key, record, bin.name, 45);
		}
	}
}
