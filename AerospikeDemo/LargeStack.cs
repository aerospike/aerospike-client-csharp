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
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class LargeStack : SyncExample
	{
		public LargeStack(Console console) : base(console)
		{
		}

		/// <summary>
		/// Perform operations on a stack within a single bin.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Large stack functions are not supported by the connected Aerospike server.");
				return;
			}

			Key key = new Key(args.ns, args.set, "stackkey");
			string binName = args.GetBinName("stackbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Initialize large stack operator.
			Aerospike.Client.LargeStack stack = client.GetLargeStack(args.policy, key, binName, null);

			// Write values.
			stack.Push(Value.Get("stackvalue1"));
			stack.Push(Value.Get("stackvalue2"));

			// Verify large stack was created with default configuration.
			Dictionary<object,object> map = stack.GetConfig();

			foreach (KeyValuePair<object,object> entry in map)
			{
				console.Info(entry.Key.ToString() + ',' + entry.Value);
			}

			int size = stack.Size();

			if (size != 2)
			{
				throw new Exception("Size mismatch. Expected 2 Received " + size);
			}

			List<object> list = stack.Peek(1);
			string received = (string)list[0];
			string expected = "stackvalue2";

			if (received != null && received.Equals(expected))
			{
				console.Info("Data matched: namespace={0} set={1} key={2} value={3}", key.ns, key.setName, key.userKey, received);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}
