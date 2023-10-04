/* 
 * Copyright 2012-2023 Aerospike, Inc.
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

namespace Aerospike.Demo
{
	public abstract class SyncExample : Example
	{
		public SyncExample(Console console)
			: base(console)
		{
		}

		public override void RunExample(Arguments args)
		{
			ClientPolicy policy = new ClientPolicy();
			policy.user = args.user;
			policy.password = args.password;
			policy.clusterName = args.clusterName;
			policy.tlsPolicy = args.tlsPolicy;
			policy.authMode = args.authMode;

			IAerospikeClient client = new AerospikeClient(policy, args.hosts);

			try
			{
				args.SetServerSpecific(client);
				RunExample(client, args);
			}
			finally
			{
				client.Close();
			}
		}

		public abstract void RunExample(IAerospikeClient client, Arguments args);
	}
}
