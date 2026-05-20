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

internal static class GetFixture
{
	public static void Setup(IAerospikeClient client, Arguments args)
	{
		Key key = new(args.ns, args.set, "docreadkey");
		Bin report = new("report", "sample-report");
		Bin location = new("location", "sample-location");
		client.Put(args.writePolicy, key, report, location);
	}

	public static void Validate(IAerospikeClient client, Arguments args)
	{
		Key key = new(args.ns, args.set, "docreadkey");

		if (!client.Exists(args.policy, key))
		{
			throw new Exception("Get verification failed: record should exist.");
		}

		Record header = client.GetHeader(args.policy, key) ?? throw new Exception("Get verification failed: header record not found.");
		Record record = client.Get(args.policy, key, "report", "location");

		if (record == null ||
			!Equals(record.GetValue("report"), "sample-report") ||
			!Equals(record.GetValue("location"), "sample-location"))
		{
			throw new Exception("Get verification failed: expected report and location bins were not returned.");
		}
	}

	public static void Cleanup(IAerospikeClient client, Arguments args)
	{
		client.Delete(args.writePolicy, new Key(args.ns, args.set, "docreadkey"));
	}
}
