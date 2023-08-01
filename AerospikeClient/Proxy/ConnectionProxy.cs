/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Socket connection wrapper.
	/// </summary>
	public class ConnectionProxy : IConnection
	{
		private static readonly String NotSupported = "Method not supported in proxy client: ";
		byte[] Payload;

		/// <summary>
		/// Create GRPC Connection class
		/// </summary>
		public ConnectionProxy(byte[] payload)
		{
			Payload = payload;
		}

		public void SetTimeout(int timeoutMillis)
		{
			throw new AerospikeException(NotSupported + "SetTimeout");
		}

		public void Write(byte[] buffer, int length)
		{
			throw new AerospikeException(NotSupported + "Write");
		}

		public void ReadFully(byte[] buffer, int length)
		{
			Array.Copy(Payload, buffer, length);
		}

		public Stream GetStream()
		{
			throw new AerospikeException(NotSupported + "GetStream");
		}

		/// <summary>
		/// Is socket closed from client perspective only.
		/// </summary>
		public bool IsClosed()
		{
			throw new AerospikeException(NotSupported + "IsClosed");
		}

		public void UpdateLastUsed()
		{
			return;
		}

		/// <summary>
		/// Shutdown and close socket.
		/// </summary>
		public void Close()
		{
			throw new AerospikeException(NotSupported + "Close");
		}
	}
}
