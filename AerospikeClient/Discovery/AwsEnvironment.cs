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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Environment read by the AWS credential chain and the default AWS endpoints. Tests override it
	/// to point the chain at stubs; production reads the process environment.
	/// </summary>
	internal class AwsEnvironment
	{
		internal static readonly AwsEnvironment System = new AwsEnvironment();

		internal virtual string GetEnv(string name)
		{
			return Environment.GetEnvironmentVariable(name);
		}

		internal virtual string UserHome()
		{
			return Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
		}

		internal virtual string Ec2Endpoint(string region)
		{
			return "https://ec2." + region + '.' + Domain(region);
		}

		internal virtual string StsEndpoint(string region)
		{
			return "https://sts." + region + '.' + Domain(region);
		}

		internal virtual string EcsEndpoint()
		{
			return "http://169.254.170.2";
		}

		internal virtual string ImdsEndpoint()
		{
			return "http://169.254.169.254";
		}

		internal virtual long CurrentTimeMillis()
		{
			return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
		}

		private static string Domain(string region)
		{
			return region.StartsWith("cn-", StringComparison.Ordinal) ? "amazonaws.com.cn" : "amazonaws.com";
		}
	}
}
