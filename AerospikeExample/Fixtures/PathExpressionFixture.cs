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

namespace Aerospike.Example;

internal static class PathExpressionFixture
{
	private const string InventoryBinName = "inventory";

	public static void Setup(IAerospikeClient client, Arguments args)
	{
		SetupInventorySample(client, ExampleFixtureSupport.Key(args, "pathexp_qs"), extraProduct: false);
		SetupInventorySample(client, ExampleFixtureSupport.Key(args, "pathexp_regex"), extraProduct: false);
		SetupInventorySample(client, ExampleFixtureSupport.Key(args, "pathexp_multi"), extraProduct: false);
		SetupInventorySample(client, ExampleFixtureSupport.Key(args, "pathexp_modify"), extraProduct: false);
		SetupInventorySample(client, ExampleFixtureSupport.Key(args, "pathexp_nofail"), extraProduct: true);
	}

	private static void SetupInventorySample(IAerospikeClient client, Key key, bool extraProduct)
	{
		Dictionary<string, object> inventory = [];

		Dictionary<string, object> product1 = new()
		{
			{ "category", "clothing" },
			{ "featured", true },
			{ "name", "Classic T-Shirt" },
			{ "description", "A lightweight cotton T-shirt perfect for everyday wear." },
			{
				"variants",
				new Dictionary<string, object>
		{
			{ "2001", new Dictionary<string, object> { { "size", "S" }, { "price", 25 }, { "quantity", 100 } } },
			{ "2002", new Dictionary<string, object> { { "size", "M" }, { "price", 25 }, { "quantity", 0 } } },
			{ "2003", new Dictionary<string, object> { { "size", "L" }, { "price", 27 }, { "quantity", 50 } } }
		}
			}
		};
		inventory.Add("10000001", product1);

		Dictionary<string, object> product2 = new()
		{
			{ "category", "clothing" },
			{ "featured", false },
			{ "name", "Casual Polo Shirt" },
			{ "description", "A soft polo shirt suitable for work or leisure." },
			{
				"variants",
				new Dictionary<string, object>
		{
			{ "2004", new Dictionary<string, object> { { "size", "M" }, { "price", 30 }, { "quantity", 20 } } },
			{ "2005", new Dictionary<string, object> { { "size", "XL" }, { "price", 32 }, { "quantity", 10 } } }
		}
			}
		};
		inventory.Add("10000002", product2);

		Dictionary<string, object> product3 = new()
		{
			{ "category", "electronics" },
			{ "featured", true },
			{ "name", "Laptop Pro 14" },
			{ "description", "High-performance laptop designed for professionals." },
			{
				"variants",
				new Dictionary<string, object>
		{
			{ "3001", new Dictionary<string, object> { { "spec", "8GB RAM" }, { "price", 599 }, { "quantity", 0 } } }
		}
			}
		};
		inventory.Add("50000006", product3);

		Dictionary<string, object> product4 = new()
		{
			{ "category", "electronics" },
			{ "featured", true },
			{ "name", "Smart TV" },
			{ "description", "Ultra HD smart television with built-in streaming apps." },
			{
				"variants",
				new List<Dictionary<string, object>>
		{
			new() { { "sku", 3007 }, { "spec", "1080p" }, { "price", 199 }, { "quantity", 60 } },
			new() { { "sku", 3008 }, { "spec", "4K" }, { "price", 399 }, { "quantity", 30 } }
		}
			}
		};
		inventory.Add("50000009", product4);

		if (extraProduct)
		{
			Dictionary<string, object> product5 = new()
			{
				{ "category", "clothing" },
				{ "featured", true },
				{ "name", "Hooded Sweatshirt" },
				{ "description", "Hooded Sweatshirt" },
				{ "variants", new Dictionary<string, object> { { "quantity", 10 } } }
			};
			inventory.Add("10000003", product5);
		}

		client.Put(null, key, new Bin(InventoryBinName, new Dictionary<string, object> { { "inventory", inventory } }));
	}
}
