/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Net;
using System.Net.Sockets;

namespace Aerospike.Client
{
	/// <summary>
	/// Async connection proxy class.
	/// </summary>
	public class AsyncConnectionProxy : IAsyncConnection
	{
		private static readonly String NotSupported = "Method not supported in proxy client: ";
		byte[] Payload;
		int Offset;

		public AsyncConnectionProxy(byte[] payload)
		{
			Payload = payload;
			Offset = 0;
		}

		public IAsyncCommand Command
		{
			get { throw new AerospikeException(NotSupported + "Command"); }
			set { throw new AerospikeException(NotSupported + "Command"); }
		}

		public DateTime LastUsed
		{
			get { throw new AerospikeException(NotSupported + "LastUsed"); }
		}

		public void Connect(IPEndPoint address)
		{
			throw new AerospikeException(NotSupported + "Connect");
		}

		public void Send(byte[] buffer, int offset, int count)
		{
			throw new AerospikeException(NotSupported + "Send");
		}

		public void Receive(byte[] buffer, int offset, int count)
		{
			if (count + Offset <= Payload.Length)
			{
				Array.Copy(Payload, Offset, buffer, 0, count);
				Offset += count;
			}
		}

		public bool IsValid()
		{
			throw new AerospikeException(NotSupported + "IsValid");
		}

		public void UpdateLastUsed()
		{
			throw new AerospikeException(NotSupported + "UpdateLastUsed");
		}

		public void Reset()
		{
			throw new AerospikeException(NotSupported + "Reset");
		}

		public void Close()
		{
			throw new AerospikeException(NotSupported + "Close");
		}
	}
}
