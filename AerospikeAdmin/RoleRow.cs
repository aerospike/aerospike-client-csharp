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
using System.ComponentModel;

namespace Aerospike.Admin
{
	public class RoleRow
	{
		public string name;
		public List<Privilege> privileges;
		public BindingList<Privilege> privilegeRows;
		public List<string> whitelist;
		public int readQuota;
		public int writeQuota;

		public RoleRow(string roleName)
		{
			this.name = roleName;
		}

		public RoleRow(Role role)
		{
			this.name = role.name;
			this.privileges = role.privileges;
			this.privilegeRows = new BindingList<Privilege>(role.privileges);
			this.whitelist = role.whitelist;
			this.readQuota = role.readQuota;
			this.writeQuota = role.writeQuota;
		}

		public string RoleName { get { return name; } }
	}
}
