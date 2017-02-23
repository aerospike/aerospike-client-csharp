/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System.Collections.Generic;
using System.ComponentModel;
using Aerospike.Client;

namespace Aerospike.Admin
{
	public class UserRow
	{
		public string name;
		public List<string> roles;
		public BindingList<RoleRow> roleRows;

		public UserRow(User user)
		{
			this.name = user.name;
			this.roles = user.roles;
			roleRows = new BindingList<RoleRow>();

			foreach (string roleName in user.roles)
			{
				roleRows.Add(new RoleRow(roleName));
			}
		}

		public string UserName { get { return name; } }
	}
}
