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
namespace Aerospike.Client
{
	/// <summary>
	/// User and assigned roles.
	/// </summary>
	public sealed class User
	{
		/// <summary>
		/// User name.
		/// </summary>
		public string name;

		/// <summary>
		/// List of assigned roles.
		/// </summary>
		public List<string> roles;

		/// <summary>
		/// List of read statistics. List may be null.
		/// Current statistics by offset are:
		/// <ul>
		/// <li>0: read quota in records per second</li>
		/// <li>1: single record read transaction rate (TPS)</li>
		/// <li>2: read scan/query record per second rate (RPS)</li>
		/// <li>3: number of limitless read scans/queries</li>
		/// </ul>
		/// Future server releases may add additional statistics.
		/// </summary>
		public List<uint> readInfo;

		/// <summary>
		/// List of write statistics. List may be null.
		/// Current statistics by offset are:
		/// <ul>
		/// <li>0: write quota in records per second</li>
		/// <li>1: single record write transaction rate (TPS)</li>
		/// <li>2: write scan/query record per second rate (RPS)</li>
		/// <li>3: number of limitless write scans/queries</li>
		/// </ul>
		/// Future server releases may add additional statistics.
		/// </summary>
		public List<uint> writeInfo;

		/// <summary>
		/// Number of currently open connections.
		/// </summary>
		public uint connsInUse;

		public override string ToString()
		{
			return "User [name=" + name + ", roles=" + roles + ", readInfo=" + readInfo + ", writeInfo=" + writeInfo + ", connsInUse=" + connsInUse + "]";
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (this.GetType() != obj.GetType())
			{
				return false;
			}
			User other = (User)obj;
			if (name == null)
			{
				if (other.name != null)
				{
					return false;
				}
			}
			else if (!name.Equals(other.name))
			{
				return false;
			}
			return true;
		}
	}
}
