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
using System.Linq;

namespace Aerospike.Client
{
	/// <summary>
	/// Unique record identifier. Records can be identified using a specified namespace,
	/// an optional set name, and a user defined key which must be unique within a set.
	/// Records can also be identified by namespace/digest which is the combination used 
	/// on the server.
	/// </summary>
	public sealed class Key
	{
		/// <summary>
		/// Namespace. Equivalent to database name.
		/// </summary>
		public readonly string ns;

		/// <summary>
		/// Optional set name. Equivalent to database table.
		/// </summary>
		public readonly string setName;

		/// <summary>
		/// Unique server hash value generated from set name and user key.
		/// </summary>
		public readonly byte[] digest;

		/// <summary>
		/// Original user key. This key is immediately converted to a hash digest.
		/// This key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		public readonly Value userKey;
	
		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, Value key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = key;

			// Some value types can't be used as keys (csblob, list, map, null).  Verify key type.
			key.ValidateKeyType();
			
			digest = ComputeDigest(setName, key);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, string key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.StringValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, byte[] key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.BytesValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <param name="offset">byte array segment offset</param>
		/// <param name="length">byte array segment length</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, byte[] key, int offset, int length)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.ByteSegmentValue(key, offset, length);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, long key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.LongValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, ulong key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.UnsignedLongValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, int key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.IntegerValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, uint key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.UnsignedIntegerValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, short key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.ShortValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, ushort key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.UnsignedShortValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, bool key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.BooleanValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, byte key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.ByteValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, optional set name and user key.
		/// The set name and user defined key are converted to a digest before sending to the server.
		/// The user key is not used or returned by the server by default. If the user key needs 
		/// to persist on the server, use one of the following methods: 
		/// <list type="bullet">
		/// <item>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
		/// and retrieved on multi-record scans and queries.</item>
		/// <item>Explicitly store and retrieve the key in a bin.</item>
		/// </list>
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">user defined unique identifier within set.</param>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public Key(string ns, string setName, sbyte key)
		{
			this.ns = ns;
			this.setName = setName;
			this.userKey = new Value.SignedByteValue(key);
			digest = ComputeDigest(setName, this.userKey);
		}

		/// <summary>
		/// Initialize key from namespace, digest, optional set name and optional userKey.
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="digest">unique server hash value</param>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="userKey">optional original user key (not hash digest)</param>
		public Key(string ns, byte[] digest, string setName, Value userKey)
		{
			this.ns = ns;
			this.digest = digest;
			this.setName = setName;
			this.userKey = userKey;
		}

		/// <summary>
		/// Hash lookup uses namespace and digest.
		/// </summary>
		public override int GetHashCode()
		{
			int result = 1;
			foreach (byte element in digest)
			{
				result = 31 * result + element;
			}
			return 31 * result + ns.GetHashCode();
		}

		/// <summary>
		/// Equality uses namespace and digest.
		/// </summary>
		public override bool Equals(object obj)
		{
			Key other = (Key) obj;

			if (digest.Length != other.digest.Length)
			{
				return false;
			}

			for (int i = 0; i < digest.Length; i++)
			{
				if (digest[i] != other.digest[i])
				{
					return false;
				}
			}
			return ns.Equals(other.ns);
		}

		/// <summary>
		/// Return key elements in string format.
		/// </summary>
		public override string ToString()
		{
			return this.ns + ":" + this.setName + ":" + this.userKey + ":" + ByteUtil.BytesToHexString(this.digest);
		}
	
		/// <summary>
		/// Generate unique server hash value from set name, key type and user defined key.  
		/// The hash function is RIPEMD-160 (a 160 bit hash).
		/// </summary>
		/// <param name="setName">optional set name, enter null when set does not exist</param>
		/// <param name="key">record identifier, unique within set</param>
		/// <returns>unique server hash value</returns>
		/// <exception cref="AerospikeException">if digest computation fails</exception>
		public static byte[] ComputeDigest(string setName, Value key)
		{
			byte[] buffer = ThreadLocalData.GetBuffer();
			int offset = ByteUtil.StringToUtf8(setName, buffer, 0);
			buffer[offset++] = (byte)key.Type;
			offset += key.Write(buffer, offset);

			return Hash.ComputeHash(buffer, offset);
		}
	}
}
