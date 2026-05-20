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

public sealed class Prepend : SyncExample
{
	/// <summary>
	/// Prepend a string to an existing string bin.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "prependkey");
		string binName = "prependbin";

		Bin initial = new(binName, "World");
		console.Info($"Initial prepend will create record. Initial value is {initial.value}.");
		client.Prepend(writePolicy, key, initial);

		Bin prefix = new(binName, "Hello ");
		console.Info($"Prepend \"{prefix.value}\" to existing record.");
		client.Prepend(writePolicy, key, prefix);
	}
}
