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
using Aerospike.Client.KVS;
using Grpc.Core;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	/// <summary>
	/// Socket connection wrapper.
	/// </summary>
	public class ConnectionProxyStream : IConnection
	{
		private static readonly String NotSupported = "Method not supported in proxy client: ";
		AsyncServerStreamingCall<AerospikeResponsePayload> Stream;
		AerospikeResponsePayload Response;
		byte[] Payload;
		int Offset;
		int BufferOffset;
		bool LastResponse;

		/// <summary>
		/// Create GRPC Connection Stream class
		/// </summary>
		public ConnectionProxyStream(AsyncServerStreamingCall<AerospikeResponsePayload> stream)
		{
			Stream = stream;
			//stream.ResponseStream.ReadAllAsync();
			stream.ResponseStream.MoveNext().Wait();
			Response = stream.ResponseStream.Current;
			if (Response.Status != 0)
			{
				throw GRPCConversions.GrpcStatusError(Response);
			}
			Payload = Response.Payload.ToByteArray();
			Offset = 0;
			LastResponse = !Response.HasNext;
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
			BufferOffset = 0;
			GRPCRead(buffer, length);
		}

		public void GRPCRead(byte[] buffer, int length)
		{
			if (Payload == null)
			{
				NextGRPCResponse();
			}
			
			if (length > Payload.Length - Offset)
			{
				// Copy remaining data
				Array.Copy(Payload, Offset, buffer, BufferOffset, Payload.Length - Offset);
				BufferOffset += Payload.Length - Offset;
				// Reset payload
				Payload = null;
				// Get the next response
				GRPCRead(buffer, length);
			}
				
			Array.Copy(Payload, Offset, buffer, BufferOffset, length);
			Offset += length;

			if (Offset >= Payload.Length)
			{
				Payload = null;
			}
		}

		private void NextGRPCResponse()
		{
			if (LastResponse)
			{
				throw new EndOfGRPCStream();
			}
			
			Stream.ResponseStream.MoveNext().Wait();
			Response = Stream.ResponseStream.Current;
			if (Response.Status != 0)
			{
				throw GRPCConversions.GrpcStatusError(Response);
			}
			Payload = Response.Payload.ToByteArray();
			Offset = 0;
			LastResponse = !Response.HasNext;
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
