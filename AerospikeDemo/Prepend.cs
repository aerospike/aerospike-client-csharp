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
	public class Prepend : SyncExample
	{
		public Prepend(Console console) : base(console)
		{
		}

		/// <summary>
		/// Prepend string to an existing string.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "prependkey");
			string binName = args.GetBinName("prependbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			Bin bin = new Bin(binName, "World");
			console.Info("Initial prepend will create record.  Initial value is " + bin.value + '.');
			client.Prepend(args.writePolicy, key, bin);

			bin = new Bin(binName, "Hello ");
			console.Info("Prepend \"" + bin.value + "\" to existing record.");
			client.Prepend(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			// The value received from the server is an unsigned byte stream.
			// Convert to an integer before comparing with expected.
			object received = record.GetValue(bin.name);
			string expected = "Hello World";

			if (received.Equals(expected))
			{
				console.Info("Prepend successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Prepend mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}
