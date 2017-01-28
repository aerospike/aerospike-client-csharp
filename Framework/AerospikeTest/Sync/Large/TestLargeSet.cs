/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
	public class TestLargeStack : TestSync
	{
		[TestMethod]
		public void LargeStack()
		{
			if (!args.ValidateLDT())
			{
				return;
			}
			Key key = new Key(args.ns, args.set, "stackkey");
			string binName = args.GetBinName("stackbin");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large stack operator.
			LargeStack stack = client.GetLargeStack(null, key, binName, null);

			// Write values.
			stack.Push(Value.Get("stackvalue1"));
			stack.Push(Value.Get("stackvalue2"));
			//stack.push(Value.get("stackvalue3"));

			// Delete last value.
			// Comment out until trim supported on server.
			//stack.trim(1);

			Assert.AreEqual(2, stack.Size());

			IList list = stack.Peek(1);
			string received = (string)list[0];
			Assert.AreEqual("stackvalue2", received);
		}	
	}
}
