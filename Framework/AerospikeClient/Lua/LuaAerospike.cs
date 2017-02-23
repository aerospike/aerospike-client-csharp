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
using Neo.IronLua;

namespace Aerospike.Client
{
	public class LuaAerospike
	{
		public static void log(int level, string message)
		{
			switch (level)
			{
				case 1:
					Log.LogMessage(Log.Level.WARN, message);
					break;
				case 2:
					Log.LogMessage(Log.Level.INFO, message);
					break;
				case 3:
				case 4:
					Log.LogMessage(Log.Level.DEBUG, message);
					break;
			}
		}

		public static void LoadLibrary(LuaInstance lua)
		{
			LuaTable table = new LuaTable();
			table["log"] = new Action<int, string>(log);

			lua.Register("aero", table);
		}
	}
}
