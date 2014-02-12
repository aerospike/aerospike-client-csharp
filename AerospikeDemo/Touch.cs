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
	public class Touch : SyncExample
	{
		public Touch(Console console) : base(console)
		{
		}

		/// <summary>
		/// Demonstrate touch command.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "touchkey");
			Bin bin = new Bin(args.GetBinName("touchbin"), "touchvalue");

			console.Info("Create record with 2 second expiration.");
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = 2;
			client.Put(writePolicy, key, bin);

			console.Info("Touch same record with 5 second expiration.");
			writePolicy.expiration = 5;
			Record record = client.Operate(writePolicy, key, Operation.Touch(), Operation.GetHeader());

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, null));
			}

			if (record.expiration == 0)
			{
				throw new Exception(string.Format("Failed to get record expiration: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			console.Info("Sleep 3 seconds.");
			Thread.Sleep(3000);

			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			console.Info("Success. Record still exists.");
			console.Info("Sleep 4 seconds.");
			Thread.Sleep(4000);

			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				console.Info("Success. Record expired as expected.");
			}
			else
			{
				console.Error("Found record when it should have expired.");
			}
		}
	}
}
