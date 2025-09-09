/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.Text;

namespace Aerospike.Admin
{
	public class UserRow
	{
		public string name;
		public List<string> roles;
		public string rolesString;
		public uint connsInUse;
		public uint readQuota;
		public uint readTps;
		public uint readQuery;
		public uint readLimitless;
		public uint writeQuota;
		public uint writeTps;
		public uint writeQuery;
		public uint writeLimitless;

		public UserRow(User user)
		{
			this.name = user.name;
			this.roles = user.roles;

			StringBuilder sb = new StringBuilder(100);
			bool comma = false;

			foreach (string role in user.roles)
			{
				if (comma)
				{
					sb.Append(", ");
				}
				else
				{
					comma = true;
				}
				sb.Append(role);
			}
			this.rolesString = sb.ToString();
			this.connsInUse = user.connsInUse;
			this.readQuota = user.readInfo[0];
			this.readTps = user.readInfo[1];
			this.readQuery = user.readInfo[2];
			this.readLimitless = user.readInfo[3];
			this.writeQuota = user.writeInfo[0];
			this.writeTps = user.writeInfo[1];
			this.writeQuery = user.writeInfo[2];
			this.writeLimitless = user.writeInfo[3];
		}

		public string UserName { get { return name; } }
		public string RolesString { get { return rolesString; } }
		public uint ConnsInUse { get { return connsInUse; } }
		public uint ReadQuota { get { return readQuota; } }
		public uint ReadTps { get { return readTps; } }
		public uint ReadQuery { get { return readQuery; } }
		public uint ReadLimitless { get { return readLimitless; } }
		public uint WriteQuota { get { return writeQuota; } }
		public uint WriteTps { get { return writeTps; } }
		public uint WriteQuery { get { return writeQuery; } }
		public uint WriteLimitless { get { return writeLimitless; } }
	}
}
