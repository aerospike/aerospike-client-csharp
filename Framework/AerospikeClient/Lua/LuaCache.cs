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
using System.Collections.Concurrent;
using System.IO;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class LuaCache
	{
		private static readonly ConcurrentQueue<LuaInstance> InstanceQueue = new ConcurrentQueue<LuaInstance>();
		private static int InstanceCount = 0;

		public static LuaInstance GetInstance()
		{
			LuaInstance instance;
				
			if (InstanceQueue.TryDequeue(out instance))
			{
				return instance;
			}
			Interlocked.Increment(ref InstanceCount);
			return new LuaInstance();
		}

		public static void PutInstance(LuaInstance instance)
		{
			int count = Interlocked.CompareExchange(ref InstanceCount, 0, 0);

			if (count <= LuaConfig.InstancePoolSize)
			{
				InstanceQueue.Enqueue(instance);
			}
			else
			{
				Interlocked.Decrement(ref InstanceCount);
				instance.Close();
			}
		}
	}
}
