/* 
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Demo
{
	internal class PathExpression(Console console) : SyncExample(console)
	{
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			RunMapKeysSelect(client, args);
			RunMapKeysWithAndFilter(client, args);
			RunInListExpression(client, args);
			RunMapKeysExpression(client, args);
			RunMapValuesExpression(client, args);
		}


		/// <summary>
		/// Use CTX.MapKeysIn to select a subset of map entries by key list
		/// via CDTOperation.SelectByPath.
		/// </summary>
		/// <param name="client">The Aerospike client.</param>
		/// <param name="args">The arguments.</param>
		private void RunMapKeysSelect(IAerospikeClient client, Arguments args)
		{
			var key = new Key(args.ns, args.set, "pathexp1");
			string binName = "mapbin";

			client.Delete(args.writePolicy, key);

			Dictionary<string, int> map = new()
			{
				{ "Charlie", 55 },
				{ "Jim", 98 },
				{ "John", 76 },
				{ "Harry", 82 }
			};

			client.Put(args.writePolicy, key, new Bin(binName, map));

			console.Info("Map: " + map);

			// Select only "Charlie" and "John" values using CTX.mapKeysIn.
			CTX ctx = CTX.MapKeysIn("Charlie", "John");
			Record record = client.Operate(
				args.writePolicy, key,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			console.Info("SelectByPath MapKeysIn [Charlie, John]: " + record.GetList(binName));
		}

		/// <summary>
		/// Use CTX.MapKeysIn combined with CTX.AndFilter to select map entries
		/// by key list and then further filter by value.
		/// </summary>
		/// <param name="client">The Aerospike client.</param>
		/// <param name="args">The arguments.</param>
		private void RunMapKeysWithAndFilter(IAerospikeClient client, Arguments args)
		{
			var key = new Key(args.ns, args.set, "pathexp2");
			string binName = "mapbin";

			Dictionary<string, int> map = new()
			{
				{ "Charlie", 55 },
				{ "Jim", 98 },
				{ "John", 76 },
				{ "Harry", 82 }
			};

			client.Put(args.writePolicy, key, new Bin(binName, map));

			console.Info("Map: " + map);

			// Select keys "Charlie", "Jim", "John", then keep only entries with value > 70.
			CTX keyCtx = CTX.MapKeysIn("Charlie", "Jim", "John");
			CTX filter = CTX.AndFilter(
				Exp.GT(
					Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(70)
				)
			);

			Record record = client.Operate(
				args.writePolicy, key,
				CDTOperation.SelectByPath(binName, SelectFlag.MAP_KEY_VALUE, keyCtx, filter)
			);

			console.Info("SelectByPath MapKeysIn [Charlie, Jim, John] AND value > 70: " + record.GetValue(binName));
		}

		/// <summary>
		/// Use Exp.InList to check if a bin value is contained in a list.
		/// </summary>
		/// <param name="client">The Aerospike client.</param>
		/// <param name="args">The arguments.</param>
		private void RunInListExpression(IAerospikeClient client, Arguments args)
		{
			var key = new Key(args.ns, args.set, "pathexp3");
			string binName = "color";
			string value = "blue";

			client.Delete(args.writePolicy, key);
			client.Put(args.writePolicy, key, new Bin(binName, value), new Bin("size", 10));

			console.Info("Record: color=blue, size=10");

			// Check if "color" bin value is in the list ["red", "blue", "green"].
			Expression exp = Exp.Build(
				Exp.InList(
					Exp.StringBin("color"),
					Exp.Val(new List<string> { "red", "blue", "green" })
				)
			);

			Record record = client.Operate(
				null, key,
				ExpOperation.Read("inList", exp, ExpReadFlags.DEFAULT)
			);

			console.Info("inList [red, blue, green] contains 'blue': " + record.GetBool("inList"));

			// Negative case: "blue" is not in ["red", "yellow", "green"].
			Expression expNot = Exp.Build(
				Exp.InList(
					Exp.StringBin("color"),
					Exp.Val(new List<string> { "red", "yellow", "green" })
				)
			);

			Record recordNot = client.Operate(
				null, key,
				ExpOperation.Read("notInList", expNot, ExpReadFlags.DEFAULT)
			);

			console.Info("inList [red, yellow, green] contains 'blue': " + recordNot.GetBool("notInList"));
		}

		/// <summary>
		/// Use Exp.MapKeysIn to extract all keys from a map bin.
		/// </summary>
		/// <param name="client">The Aerospike client.</param>
		/// <param name="args">The arguments.</param>
		private void RunMapKeysExpression(IAerospikeClient client, Arguments args)
		{
			var key = new Key(args.ns, args.set, "pathexp4");
			string binName = "mapbin";

			client.Delete(args.writePolicy, key);

			Dictionary<string, int> map = new()
			{
				{ "Charlie", 55 },
				{ "Jim", 98 },
				{ "John", 76 }
			};
			client.Put(args.writePolicy, key, new Bin(binName, map));

			console.Info("Map: " + map);

			// Extract all keys from the map.
			Expression exp = Exp.Build(
				Exp.MapKeysIn(Exp.MapBin(binName))
			);

			Record record = client.Operate(null, key,
				ExpOperation.Read("keys", exp, ExpReadFlags.DEFAULT)
			);

			List<object> keys = (List<object>)record.GetList("keys");
			console.Info("Exp.mapKeysIn: " + keys);
		}

		/// <summary>
		/// Use Exp.MapValues to extract all values from a map bin.
		/// </summary>
		/// <param name="client">The Aerospike client.</param>
		/// <param name="args">The arguments.</param>
		private void RunMapValuesExpression(IAerospikeClient client, Arguments args)
		{
			var key = new Key(args.ns, args.set, "pathexp5");
			string binName = "mapbin";

			client.Delete(args.writePolicy, key);

			Dictionary<string, int> map = new()
			{
				{ "Charlie", 55 },
				{ "Jim", 98 },
				{ "John", 76 }
			};

			client.Put(args.writePolicy, key, new Bin(binName, map));

			console.Info("Map: " + map);

			// Extract all values from the map.
			Expression exp = Exp.Build(
				Exp.MapValuesIn(Exp.MapBin(binName))
			);

			Record record = client.Operate(null, key,
				ExpOperation.Read("values", exp, ExpReadFlags.DEFAULT)
			);

			List<object> values = (List<object>)record.GetList("values");
			console.Info("Exp.MapValues: " + values);
		}
	}
}
