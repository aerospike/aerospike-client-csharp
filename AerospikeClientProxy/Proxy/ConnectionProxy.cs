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
using Aerospike.Client.Proxy.KVS;

namespace Aerospike.Client.Proxy
{
	/// <summary>
	/// Connection wrapper for GRPC payload.
	/// </summary>
	public class ConnectionProxy : IConnection
	{
		private static readonly String NotSupported = "Method not supported in proxy client: ";
		private byte[] Payload { get; }
		private int Offset { get; set; }

		/// <summary>
		/// Create GRPC Connection class
		/// </summary>
		public ConnectionProxy(AerospikeResponsePayload response)
		{
			if (response.Status != 0)
			{
				throw GRPCConversions.GrpcStatusError(response);
			}
			Payload = response.Payload.ToByteArray();
			Offset = 0;
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
			Array.Copy(Payload, Offset, buffer, 0, length);
			Offset += length;
		}

		public void ReadFully(byte[] buffer, int length, byte state)
		{
			throw new AerospikeException(NotSupported + "ReadFully");
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
