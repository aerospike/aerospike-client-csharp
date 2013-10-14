/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System.Collections.Concurrent;
using LuaInterface;

namespace Aerospike.Client
{
	public sealed class LuaCache
	{
		private static readonly BlockingCollection<LuaInstance> InstanceQueue = new BlockingCollection<LuaInstance>(LuaConfig.InstancePoolSize);

		public static LuaInstance GetInstance()
		{
			LuaInstance instance;
				
			if (InstanceQueue.TryTake(out instance))
			{
				return instance;
			}
			return new LuaInstance();
		}

		public static void PutInstance(LuaInstance instance)
		{
			InstanceQueue.TryAdd(instance);
		}
	}
}