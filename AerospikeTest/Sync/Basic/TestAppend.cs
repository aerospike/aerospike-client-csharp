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
	public class TestAppend : TestSync
	{
		[TestMethod]
		public void Append()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "appendkey");
			string binName = Suite.GetBinName("appendbin");

			// Delete record if it already exists.
			client.Delete(null, key);

			Bin bin = new(binName, "Hello");
			client.Append(null, key, bin);

			bin = new Bin(binName, " World");
			client.Append(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin.name, "Hello World");
		}

		[TestMethod]
		public void Prepend()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "prependkey");
			string binName = Suite.GetBinName("prependbin");

			// Delete record if it already exists.
			client.Delete(null, key);

			Bin bin = new(binName, "World");
			client.Prepend(null, key, bin);

			bin = new Bin(binName, "Hello ");
			client.Prepend(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin.name, "Hello World");
		}
	}
}
