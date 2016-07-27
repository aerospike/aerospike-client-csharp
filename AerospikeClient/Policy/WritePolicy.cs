/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
	/// Container object for policy attributes used in write operations.
	/// This object is passed into methods where database writes can occur.
	/// </summary>
	public sealed class WritePolicy : Policy
	{
		/// <summary>
		/// Qualify how to handle writes where the record already exists.
		/// </summary>
		public RecordExistsAction recordExistsAction = RecordExistsAction.UPDATE;

		/// <summary>
		/// Qualify how to handle record writes based on record generation. The default (NONE)
		/// indicates that the generation is not used to restrict writes.
		/// </summary>
		public GenerationPolicy generationPolicy = GenerationPolicy.NONE;

		/// <summary>
		/// Desired consistency guarantee when committing a transaction on the server. The default 
		/// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to 
		/// be successful before returning success to the client.
		/// </summary>
		public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;

		/// <summary>
		/// Expected generation. Generation is the number of times a record has been modified
		/// (including creation) on the server. If a write operation is creating a record, 
		/// the expected generation would be 0.  
		/// </summary>
		public int generation;

		/// <summary>
		/// Record expiration.  Also known as ttl (time to live). 
		/// Seconds record will live before being removed by the server.
		/// <para>
		/// Expiration values:
		/// <list type="bullet">
		/// <item>-1: Never expire. Supported by Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server versions >= 3.1.4.</item>
		/// <item>0:  Default to namespace's "default-ttl" on the server.</item>
		/// <item>> 0: Actual expiration in seconds.</item>
		/// </list>
		/// </para>
		/// </summary>
		public int expiration;

		/// <summary>
		/// For client operate(), return a result for every operation.
		/// <para>
		/// Some operations do not return results by default (ListOperation.clear() for example).
		/// This can make it difficult to determine the desired result offset in the returned
		/// bin's result list.
		/// </para>
		/// <para>
		/// Setting respondAllOps to true makes it easier to identify the desired result offset 
		/// (result offset equals bin's operate sequence).  If there is a map operation in operate(),
		/// respondAllOps will be forced to true for that operate() call.
		/// </para>
		/// <para>
		/// Default: false
		/// </para>
		/// </summary>
		public bool respondAllOps;

		/// <summary>
		/// If the transaction results in a record deletion, leave a tombstone for the record.
		/// This prevents deleted records from reappearing after node failures.
		/// Valid for Aerospike Server Enterprise Edition 4+ only.
		/// <para>
		/// Default: false (do not tombstone deleted records).
		/// </para>
		/// </summary>
		public bool durableDelete;
	
		/// <summary>
		/// Copy write policy from another write policy.
		/// </summary>
		public WritePolicy(WritePolicy other)
			: base(other)
		{
			this.recordExistsAction = other.recordExistsAction;
			this.generationPolicy = other.generationPolicy;
			this.commitLevel = other.commitLevel;
			this.generation = other.generation;
			this.expiration = other.expiration;
			this.respondAllOps = other.respondAllOps;
			this.durableDelete = other.durableDelete;
		}

		/// <summary>
		/// Copy write policy from another policy.
		/// </summary>
		public WritePolicy(Policy other)
			: base(other)
		{
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public WritePolicy()
		{
		}
	}
}
