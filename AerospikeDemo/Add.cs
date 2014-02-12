/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Add : SyncExample
	{
		public Add(Console console) : base(console)
		{
		}

		/// <summary>
		/// Add integer values.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "addkey");
			string binName = args.GetBinName("addbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Perform some adds and check results.
			Bin bin = new Bin(binName, 10);
			console.Info("Initial add will create record.  Initial value is " + bin.value + '.');
			client.Add(args.writePolicy, key, bin);

			bin = new Bin(binName, 5);
			console.Info("Add " + bin.value + " to existing record.");
			client.Add(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			// The value received from the server is an unsigned byte stream.
			// Convert to an integer before comparing with expected.
			object obj = record.GetValue(bin.name);
			int received = (int)(long)obj;		
			int expected = 15;

			if (received == expected)
			{
				console.Info("Add successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Add mismatch: Expected {0}. Received {1}.", expected, received);
			}

			// Demonstrate add and get combined.
			bin = new Bin(binName, 30);
			console.Info("Add " + bin.value + " to existing record.");
			record = client.Operate(args.writePolicy, key, Operation.Add(bin), Operation.Get(bin.name));

			expected = 45;
			received = (int)(long)record.GetValue(bin.name);

			if (received == expected)
			{
				console.Info("Add successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Add mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}
