/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using System.Linq;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Admin
{
	public class UserRow
	{
		public string user;
		public List<string> roles;
		public string rolesString;

		public UserRow(UserRoles userRoles)
		{
			this.user = userRoles.user;
			this.roles = userRoles.roles;

			StringBuilder sb = new StringBuilder(100);
			List<string> roleList = userRoles.roles;

			for (int i = 0; i < roleList.Count; i++)
			{
				if (i > 0)
				{
					sb.Append(", ");
				}
				sb.Append(roleList[i]);
			}
			this.rolesString = sb.ToString(); 
		}

		public string User { get { return user; } }
		public string Roles { get { return rolesString; } }
	}
}
