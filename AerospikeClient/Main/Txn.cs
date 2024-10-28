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
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Mutli-record transaction (MRT). Each command in the MRT must use the same namespace.
	/// </summary>
	public class Txn
	{
		/// <summary>
		/// MRT State.
		/// </summary>
		public enum TxnState
		{
			OPEN,
			VERIFIED,
			COMMITTED,
			ABORTED
		}

		private static long randomState = DateTime.UtcNow.Ticks;

		public long Id { get; private set; }
		public ConcurrentHashMap<Key, long> Reads { get; private set; }
		public ConcurrentHashSet<Key> Writes { get; private set; }
		public TxnState State { get; internal set; }
		public string Ns { get; private set; }

		/// <summary>
		/// MRT deadline. The deadline is a wall clock time calculated by the server from the
		/// MRT timeout that is sent by the client when creating the MRT monitor record. This deadline
		/// is used to avoid client/server clock skew issues. For internal use only.
		/// </summary>
		internal int Deadline { get; set; }
		
		/// <summary>
		/// MRT timeout in seconds. The timer starts when the MRT monitor record is created.
		/// This occurs when the first command in the MRT is executed. If the timeout is reached before
		/// a commit or abort is called, the server will expire and rollback the MRT.
		/// </summary>
		public int Timeout { get; set; }

		private bool monitorInDoubt;

		public bool InDoubt { get; internal set; }

		/// <summary>
		/// Create MRT, assign random transaction id and initialize reads/writes hashmaps with 
		/// default capacities. The default MRT timeout is 10 seconds.
		/// </summary>
		public Txn()
		{
			Id = CreateId();
			Reads = new ConcurrentHashMap<Key, long>();
			Writes = new ConcurrentHashSet<Key>();
			Deadline = 0;
			State = TxnState.OPEN;
			Timeout = 10; // seconds
		}

		/// <summary>
		/// Create MRT, assign random transaction id and initialize reads/writes hashmaps with 
		/// given capacities. The default MRT timeout is 10 seconds.
		/// </summary>
		/// <param name="readsCapacity">expected number of record reads in the MRT. Minimum value is 16.</param>
		/// <param name="writesCapacity">expected number of record writes in the MRT. Minimum value is 16.</param>
		public Txn(int readsCapacity, int writesCapacity)
		{
			if (readsCapacity < 16)
			{
				readsCapacity = 16; // TODO ask Richard and Brian about this
			}

            if (writesCapacity < 16)
			{
				writesCapacity = 16;
			}

			Id = CreateId();
			Reads = new ConcurrentHashMap<Key, long>(readsCapacity);
			Writes = new ConcurrentHashSet<Key>(writesCapacity);
			Deadline = 0;
			State = TxnState.OPEN;
			Timeout = 10; // seconds
		}

		[System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
		private static long UnsignedRightShift(long n, int s) => n >= 0 ? n >> s : (n >> s) + (2 << ~s);

		private static long CreateId()
		{
			long oldState, newState, interlockedResult;
			do
			{
				oldState = Interlocked.Read(ref randomState);
				newState = oldState;
				newState ^= UnsignedRightShift(newState, 12);
				newState ^= newState << 25;
				newState ^= UnsignedRightShift(newState, 27);
				interlockedResult = Interlocked.CompareExchange(ref randomState, newState, oldState);
			} while (oldState != interlockedResult);
			return newState * 0x2545f4914f6cdd1dL;
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
					Reads.Remove(key);
					Writes.Add(key);
				}
			}
		}

		/// <summary>
		/// Add key to write hash when write command is in doubt (usually caused by timeout).
		/// </summary>
		public void OnWriteInDoubt(Key key)
		{
			Reads.Remove(key);
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

		public void Clear()
		{
			Ns = null;
			Deadline = 0;
			Reads.Clear();
			Writes.Clear();
		}
	}
}
