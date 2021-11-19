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
	/// Policy attributes used in batch read commands.
	/// </summary>
	public sealed class BatchReadPolicy
	{
		/// <summary>
		/// Read policy for AP (availability) namespaces.
		/// <para>
		/// Default: <see cref="Aerospike.Client.ReadModeAP.ONE"/>
		/// </para>
		/// </summary>
		public ReadModeAP readModeAP = ReadModeAP.ONE;

		/// <summary>
		/// Read policy for SC (strong consistency) namespaces.
		/// <para>
		/// Default: <see cref="Aerospike.Client.ReadModeSC.SESSION"/>
		/// </para>
		/// </summary>
		public ReadModeSC readModeSC = ReadModeSC.SESSION;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public BatchReadPolicy(BatchReadPolicy other)
		{
			this.readModeAP = other.readModeAP;
			this.readModeSC = other.readModeSC;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public BatchReadPolicy()
		{
		}
	}
}
