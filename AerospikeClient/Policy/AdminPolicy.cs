/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
	/// Policy attributes used for user administration commands.
	/// </summary>
	public class AdminPolicy
	{
		/// <summary>
		/// User administration command socket timeout in milliseconds.
		/// <para>Default: 0 (no timeout)</para>
		/// </summary>
		public int timeout;

		/// <summary>
		/// Copy admin policy from another admin policy.
		/// </summary>
		public AdminPolicy(AdminPolicy other)
		{
			this.timeout = other.timeout;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public AdminPolicy()
		{
		}

		/// <summary>
		/// Creates a deep copy of this admin policy.
		/// </summary>
		/// <returns></returns>
		public AdminPolicy Clone()
		{
			return new AdminPolicy(this);
		}
	}
}
