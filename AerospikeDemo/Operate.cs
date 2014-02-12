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
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Operate : SyncExample
	{
		public Operate(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Demonstrate multiple operations on a single record in one call.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			// Write initial record.
			Key key = new Key(args.ns, args.set, "opkey");
			Bin bin1 = new Bin("optintbin", 7);
			Bin bin2 = new Bin("optstringbin", "string value");
			console.Info("Put: namespace={0} set={1} key={2} binname1={3} binvalue1={4} binname1={5} binvalue1={6}",
				key.ns, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);
			client.Put(args.writePolicy, key, bin1, bin2);

			// Add integer, write new string and read record.
			Bin bin3 = new Bin(bin1.name, 4);
			Bin bin4 = new Bin(bin2.name, "new string");
			console.Info("Add: " + bin3.value);
			console.Info("Write: " + bin4.value);
			console.Info("Read:");
			Record record = client.Operate(args.writePolicy, key, Operation.Add(bin3), Operation.Put(bin4), Operation.Get());

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			ValidateBin(key, record, bin3.name, 11L, record.GetValue(bin3.name));
			ValidateBin(key, record, bin4.name, bin4.value.ToString(), record.GetValue(bin4.name));
		}

		private void ValidateBin(Key key, Record record, string binName, object expected, object received)
		{
			if (received != null && received.Equals(expected))
			{
				console.Info("Bin matched: namespace={0} set={1} key={2} bin={3} value={4} generation={5} expiration={6}",
					key.ns, key.setName, key.userKey, binName, received, record.generation, record.expiration);
			}
			else
			{
				console.Error("Bin mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}
