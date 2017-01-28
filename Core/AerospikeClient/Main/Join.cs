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

namespace Aerospike.Client
{
	/// <summary>
	/// Client-side join definition. The left record must contain a bin with a 
	/// list of keys.  That list will be used to retreive other records in the
	/// given namespace and set.
	/// </summary>
	public sealed class Join
	{
		internal string leftKeysBinName;
		internal string rightNamespace;
		internal string rightSetName;
		internal string[] rightBinNames;

		/// <summary>
		/// Create join definition to read all joined bins.
		/// </summary>
		/// <param name="leftKeysBinName">bin name of key list located in left record</param>
		/// <param name="rightNamespace">namespace of joined record(s)</param>
		/// <param name="rightSetName">set name of joined record(s)</param>
		public Join(string leftKeysBinName, string rightNamespace, string rightSetName)
		{
			this.leftKeysBinName = leftKeysBinName;
			this.rightNamespace = rightNamespace;
			this.rightSetName = rightSetName;
		}

		/// <summary>
		/// Create join definition to read specified joined bins.
		/// </summary>
		/// <param name="leftKeysBinName">bin name of key list located in main record</param>
		/// <param name="rightNamespace">namespace of joined record(s)</param>
		/// <param name="rightSetName">set name of joined record(s)</param>
		/// <param name="rightBinNames">bin names to retrieved in joined record(s)</param>
		public Join(string leftKeysBinName, string rightNamespace, string rightSetName, params string[] rightBinNames)
		{
			this.leftKeysBinName = leftKeysBinName;
			this.rightNamespace = rightNamespace;
			this.rightSetName = rightSetName;
			this.rightBinNames = rightBinNames;
		}
	}
}
