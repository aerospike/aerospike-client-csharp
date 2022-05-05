/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	public abstract class AsyncExample : Example
	{
		public AsyncExample(Console console)
			: base(console)
		{
		}

		public override void RunExample(Arguments args)
		{
			AsyncClientPolicy policy = new AsyncClientPolicy();
			policy.user = args.user;
			policy.password = args.password;
			policy.clusterName = args.clusterName;
			policy.tlsPolicy = args.tlsPolicy;
			policy.authMode = args.authMode;
			policy.asyncMaxCommands = args.commandMax;
			policy.failIfNotConnected = true;

			AsyncClient client = new AsyncClient(policy, args.hosts);

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

		public abstract void RunExample(AsyncClient client, Arguments args);
	}
}
