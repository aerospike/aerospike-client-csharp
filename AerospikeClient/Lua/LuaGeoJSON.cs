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
using System;

namespace Aerospike.Client
{
	public class LuaGeoJSON : LuaData
	{
		private string value;

		public LuaGeoJSON(string value)
		{
			this.value = value;
		}

		public LuaGeoJSON()
		{
			value = "";
		}

		public object LuaToObject()
		{
			return value;
		}

		public override string ToString()
		{
			return value;
		}

		public static int size(LuaGeoJSON value)
		{
			return value.value.Length;
		}
	}
}
