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
using Aerospike.Client;

namespace Aerospike.Test
{
	public class TestAppend : TestSync
	{
		[Xunit.Fact]
		public void Append()
		{
			Key key = new Key(args.ns, args.set, "appendkey");
			string binName = args.GetBinName("appendbin");

			// Delete record if it already exists.
			client.Delete(null, key);

			Bin bin = new Bin(binName, "Hello");
			client.Append(null, key, bin);

			bin = new Bin(binName, " World");
			client.Append(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin.name, "Hello World");
		}

		[Xunit.Fact]
		public void Prepend()
		{
			Key key = new Key(args.ns, args.set, "prependkey");
			string binName = args.GetBinName("prependbin");

			// Delete record if it already exists.
			client.Delete(null, key);

			Bin bin = new Bin(binName, "World");
			client.Prepend(null, key, bin);

			bin = new Bin(binName, "Hello ");
			client.Prepend(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin.name, "Hello World");
		}
	}
}
