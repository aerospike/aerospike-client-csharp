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
	public class Expire : SyncExample
	{
		public Expire(Console console) : base(console)
		{
		}

		/// <summary>
		/// Write and twice read a bin value, demonstrating record expiration.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "expirekey");
			Bin bin = new Bin(args.GetBinName("expirebin"), "expirevalue");

			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4} expiration=2", 
				key.ns, key.setName, key.userKey, bin.name, bin.value);

			// Specify that record expires 2 seconds after it's written.
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = 2;
			client.Put(writePolicy, key, bin);

			// Read the record before it expires, showing it's there.
			console.Info("Get: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

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
				console.Info("Get successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				throw new Exception(string.Format("Expire mismatch: Expected {0}. Received {1}.", expected, received));
			}

			// Read the record after it expires, showing it's gone.
			console.Info("Sleeping for 3 seconds ...");
			Thread.Sleep(3 * 1000);
			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				console.Info("Expiry successful. Record not found.");
			}
			else
			{
				console.Error("Found record when it should have expired.");
			}
		}
	}
}
