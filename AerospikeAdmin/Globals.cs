/* 
 * Copyright 2012-2018 Aerospike, Inc.
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

namespace Aerospike.Admin
{
	public class Globals
	{
		private static volatile List<Role> Roles;

		public static void RefreshRoles(AerospikeClient client, User user, bool admin)
		{
			if (admin)
			{
				// Query all roles
				Roles = client.QueryRoles(null);
			}
			else
			{
				List<Role> list = new List<Role>(user.roles.Count);

				foreach (string roleName in user.roles)
				{
					Role role = client.QueryRole(null, roleName);
					list.Add(role);
				}
				Roles = list;
			}
		}

		public static List<Role> GetAllRoles()
		{
			return Roles;
		}

		public static List<Role> GetCustomRoles()
		{
			List<Role> allRoles = Roles;
			List<Role> roles = new List<Role>(allRoles.Count);

			foreach (Role role in allRoles)
			{
				if (!role.isPredefined())
				{
					roles.Add(role);
				}
			}
			return roles;
		}

		public static List<Role> FindRoles(List<string> filters)
		{
			List<Role> allRoles = Roles;
			List<Role> roles = new List<Role>(filters.Count);

			foreach (string roleName in filters)
			{
				Role role = FindRole(allRoles, roleName);

				if (role != null)
				{
					roles.Add(role);
				}
			}
			return roles;
		}

		private static Role FindRole(List<Role> roles, string search)
		{
			// Use binary search to find row.  Assume sorted.
			int lower = 0;
			int upper = roles.Count - 1;
			int mid;
			int cmp;

			while (lower <= upper)
			{
				mid = (lower + upper) / 2;
				cmp = roles[mid].name.CompareTo(search);

				if (cmp < 0)
				{
					lower = mid + 1;
					continue;
				}
				else if (cmp > 0)
				{
					upper = mid - 1;
					continue;
				}
				else
				{
					return roles[mid];
				}
			}
			return null;
		}
	}
}
