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

using System;
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	/// <summary>
	/// Mutli-record transaction (MRT). Each command in the MRT must use the same namespace.
	/// </summary>
	public class Txn
	{
		public long Id { get; private set; }
		public ConcurrentDictionary<Key, long> Reads { get; private set; }
		public HashSet<Key> Writes { get; private set; }
		public string Ns { get; private set; }
		public int Deadline { get; set; }

		private bool monitorInDoubt;

		private bool rollAttempted;

		/// <summary>
		/// Create MRT, assign random transaction id and initialize reads/writes hashmaps with default capacities.
		/// </summary>
		public Txn()
		{
			Id = CreateId();
			Reads = new ConcurrentDictionary<Key, long>();
			Writes = new HashSet<Key>();
			Deadline = 0;
		}

		/// <summary>
		/// Create MRT, assign random transaction id and initialize reads/writes hashmaps with given capacities.
		/// </summary>
		/// <param name="readsCapacity">expected number of record reads in the MRT. Minimum value is 16.</param>
		/// <param name="writesCapacity">expected number of record writes in the MRT. Minimum value is 16.</param>
		public Txn(int readsCapacity, int writesCapacity)
		{
			if (readsCapacity < 16)
			{
				readsCapacity = 16;
			}

            if (writesCapacity < 16)
			{
				writesCapacity = 16;
			}

			Id = CreateId();
			Reads = new ConcurrentDictionary<Key, long>(100, readsCapacity);
			Writes = new HashSet<Key>(writesCapacity);
		}

		private static long CreateId()
		{
			// An id of zero is considered invalid. Create random numbers
			// in a loop until non-zero is returned.
			Random r = new();
			long id = r.NextInt64();

			while (id == 0)
			{
				id = r.NextInt64();
			}
			return id;
		}

		/// <summary>
		/// Process the results of a record read. For internal use only.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="version"></param>
		internal void OnRead(Key key, long? version)
		{
			if (version.HasValue)
			{
				Reads.TryAdd(key, version.Value);
			}
		}

		/// <summary>
		/// Get record version for a given key.
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		public long? GetReadVersion(Key key)
		{
			if (Reads.ContainsKey(key))
			{
				return Reads[key];
			}
			else
			{
				return null;
			}
		}

		/// <summary>
		/// Process the results of a record write. For internal use only.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="version"></param>
		/// <param name="resultCode"></param>
		public void OnWrite(Key key, long? version, int resultCode)
		{
			// Write commands set namespace prior to sending the command, so there is
			// no need to call it here when receiving the response.
			if (version.HasValue)
			{
				Reads.TryAdd(key, version.Value);
			}
			else
			{
				if (resultCode == ResultCode.OK)
				{
					Reads.Remove(key, out _);
					Writes.Add(key);
				}
			}
		}

		/// <summary>
		/// Add key to write hash when write command is in doubt (usually caused by timeout).
		/// </summary>
		public void OnWriteInDoubt(Key key)
		{
			Reads.Remove(key, out _);
			Writes.Add(key);
		}

		/// <summary>
		/// Set MRT namespace only if doesn't already exist.
		/// If namespace already exists, verify new namespace is the same.
		/// </summary>
		public void SetNamespace(string ns)
		{
			if (Ns == null) 
			{
				Ns = ns;
			}
			else if (!Ns.Equals(ns)) {
				throw new AerospikeException("Namespace must be the same for all commands in the MRT. orig: " +
					Ns + " new: " + ns);
			}
		}

		/// <summary>
		/// Set MRT namespaces for each key only if doesn't already exist.
		/// If namespace already exists, verify new namespace is the same.
		/// </summary>
		public void SetNamespace(Key[] keys)
		{
			foreach (Key key in keys)
			{
				SetNamespace(key.ns);
			}
		}

		/// <summary>
		/// Set MRT namespaces for each key only if doesn't already exist.
		/// If namespace already exists, verify new namespace is the same.
		/// </summary>
		public void SetNamespace(List<BatchRead> records)
		{
			foreach (BatchRead br in records)
			{
				SetNamespace(br.key.ns);
			}
		}

		/// <summary>
		/// Set that the MRT monitor existence is in doubt.
		/// </summary>
		public void SetMonitorInDoubt()
		{
			this.monitorInDoubt = true;
		}

		/// <summary>
		/// Does MRT monitor record exist or is in doubt.
		/// </summary>
		public bool MonitorMightExist()
		{
			return Deadline != 0 || monitorInDoubt;
		}

		/// <summary>
		/// Does MRT monitor record exist.
		/// </summary>
		public bool MonitorExists()
		{
			return Deadline != 0;
		}

		public bool SetRollAttempted()
		{
			if (rollAttempted)
			{
				return false;
			}
			rollAttempted = true;
			return true;
		}

		public void Clear()
		{
			Ns = null;
			Deadline = 0;
			Reads.Clear();
			Writes.Clear();
		}
	}
}
