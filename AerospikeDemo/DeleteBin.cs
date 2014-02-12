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
	public class DeleteBin : SyncExample
	{
		public DeleteBin(Console console) : base(console)
		{
		}

		/// <summary>
		/// Drop a bin from a record.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (args.singleBin)
			{
				console.Info("Delete bin is not applicable to single bin servers.");
				return;
			}

			console.Info("Write multi-bin record.");
			Key key = new Key(args.ns, args.set, "delbinkey");
			string binName1 = args.GetBinName("bin1");
			string binName2 = args.GetBinName("bin2");
			Bin bin1 = new Bin(binName1, "value1");
			Bin bin2 = new Bin(binName2, "value2");
			client.Put(args.writePolicy, key, bin1, bin2);

			console.Info("Delete one bin in the record.");
			bin1 = Bin.AsNull(binName1); // Set bin value to null to drop bin.
			client.Put(args.writePolicy, key, bin1);

			console.Info("Read record.");
			Record record = client.Get(args.policy, key, bin1.name, bin2.name, "bin3");

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			foreach (KeyValuePair<string, object> entry in record.bins)
			{
				console.Info("Received: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, entry.Key, entry.Value);
			}

			bool valid = true;

			if (record.GetValue("bin1") != null)
			{
				console.Error("bin1 still exists.");
				valid = false;
			}

			object v2 = record.GetValue("bin2");

			if (v2 == null || !v2.Equals("value2"))
			{
				console.Error("bin2 value mismatch.");
				valid = false;
			}

			if (valid)
			{
				console.Info("Bin delete successful");
			}
		}
	}
}
