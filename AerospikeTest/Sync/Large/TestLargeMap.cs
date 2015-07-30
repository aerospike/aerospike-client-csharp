/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	public class TestLargeMap : TestSync
	{
		[TestMethod]
		public void LargeMap()
		{
			Key key = new Key(args.ns, args.set, "setkey");
			string binName = args.GetBinName("setbin");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize Large Map operator.
			LargeMap lmap = client.GetLargeMap(null, key, binName, null);

			// Write values.
			lmap.Put(Value.Get("lmapName1"), Value.Get("lmapValue1"));
			lmap.Put(Value.Get("lmapName2"), Value.Get("lmapValue2"));
			lmap.Put(Value.Get("lmapName3"), Value.Get("lmapValue3"));

			// Remove last value.
			lmap.Remove(Value.Get("lmapName3"));
			Assert.AreEqual(2, lmap.Size());

			IDictionary mapReceived = lmap.Get(Value.Get("lmapName2"));
			string stringReceived = (string)mapReceived["lmapName2"];
			Assert.AreEqual("lmapValue2", stringReceived);
		}	
	}
}
