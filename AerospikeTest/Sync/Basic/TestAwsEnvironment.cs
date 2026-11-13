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
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Environment seam pointing the AWS credential chain at a <see cref="StubAwsServer"/>.
	/// </summary>
	internal sealed class TestAwsEnvironment : AwsEnvironment
	{
		internal readonly Dictionary<string, string> Vars = new Dictionary<string, string>();
		internal readonly string Endpoint;
		internal string Home = "/nonexistent-home";
		internal long ClockOffset;

		internal TestAwsEnvironment(string endpoint)
		{
			Endpoint = endpoint;
		}

		internal TestAwsEnvironment Set(string name, string value)
		{
			Vars[name] = value;
			return this;
		}

		internal override string GetEnv(string name)
		{
			return Vars.TryGetValue(name, out string value) ? value : null;
		}

		internal override string UserHome()
		{
			return Home;
		}

		internal override string Ec2Endpoint(string region)
		{
			return Endpoint;
		}

		internal override string StsEndpoint(string region)
		{
			return Endpoint + "/sts";
		}

		internal override string EcsEndpoint()
		{
			return Endpoint + "/ecs";
		}

		internal override string ImdsEndpoint()
		{
			return Endpoint;
		}

		internal override long CurrentTimeMillis()
		{
			return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + ClockOffset;
		}
	}
}
