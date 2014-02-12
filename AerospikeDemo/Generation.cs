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
	public class Generation : SyncExample
	{
		public Generation(Console console) : base(console)
		{
		}

		/// <summary>
		/// Exercise record generation functionality.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "genkey");
			string binName = args.GetBinName("genbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Set some values for the same record.
			Bin bin = new Bin(binName, "genvalue1");
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value);

			client.Put(args.writePolicy, key, bin);

			bin = new Bin(binName, "genvalue2");
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value);

			client.Put(args.writePolicy, key, bin);

			// Retrieve record and its generation count.
			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			object received = record.GetValue(bin.name);
			string expected = bin.value.ToString();

			if (received.Equals(expected))
			{
				console.Info("Get successful: namespace={0} set={1} key={2} bin={3} value={4} generation={5}", 
					key.ns, key.setName, key.userKey, bin.name, received, record.generation);
			}
			else
			{
				throw new Exception(string.Format("Get mismatch: Expected {0}. Received {1}.", expected, received));
			}

			// Set record and fail if it's not the expected generation.
			bin = new Bin(binName, "genvalue3");
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4} expected generation={5}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value, record.generation);

			WritePolicy writePolicy = new WritePolicy();
			writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
			writePolicy.generation = record.generation;
			client.Put(writePolicy, key, bin);

			// Set record with invalid generation and check results .
			bin = new Bin(binName, "genvalue4");
			writePolicy.generation = 9999;
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4} expected generation={5}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value, writePolicy.generation);

			try
			{
				client.Put(writePolicy, key, bin);
				throw new Exception("Should have received generation error instead of success.");
			}
			catch (AerospikeException ae)
			{
				if (ae.Result == ResultCode.GENERATION_ERROR)
				{
					console.Info("Success: Generation error returned as expected.");
				}
				else
				{
					throw new Exception(string.Format("Unexpected set return code: namespace={0} set={1} key={2} bin={3} value={4} code={5}", 
						key.ns, key.setName, key.userKey, bin.name, bin.value, ae.Result));
				}
			}

			// Verify results.
			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			received = record.GetValue(bin.name);
			expected = "genvalue3";

			if (received.Equals(expected))
			{
				console.Info("Get successful: namespace={0} set={1} key={2} bin={3} value={4} generation={5}", 
					key.ns, key.setName, key.userKey, bin.name, received, record.generation);
			}
			else
			{
				throw new Exception(string.Format("Get mismatch: Expected {0}. Received {1}.", expected, received));
			}
		}
	}
}
