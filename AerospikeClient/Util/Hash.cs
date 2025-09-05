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
namespace Aerospike.Client
{
	/// <summary>
	/// This class contains the algorithm used to create hash digests from user keys.
	/// </summary>
	public class Hash
	{
#if NETFRAMEWORK
		private static readonly ThreadLocal<HashAlgorithm> ThreadLocalAlgorithm = new ThreadLocal<HashAlgorithm>(() =>
		{
			return RIPEMD160Managed.Create();
		});

		public static byte[] ComputeHash(byte[] buffer, int length)
		{
			// Benchmarks show thread local implementation is faster than creating instance every time
			// because RIPEMD160Managed.Create() is more expensive than the overhead of retrieving thread
			// local instance.
			return ThreadLocalAlgorithm.Value.ComputeHash(buffer, 0, length);
		}
#else
		public static byte[] ComputeHash(byte[] buffer, int length)
		{
			// Benchmarks show creating instance every time is faster than thread local
			// implementation because instance implementation puts ValueRipemd160 on
			// stack (fast) and eliminates the overhead of retrieving thread local instance.
			return ValueRipemd160.ComputeHashDigest(buffer.AsSpan(0, length));
		}
#endif
	}
}