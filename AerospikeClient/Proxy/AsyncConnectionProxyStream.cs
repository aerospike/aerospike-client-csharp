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
using Aerospike.Client.KVS;
using Grpc.Core;
using System;
using System.Net;
using System.Net.Sockets;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	/// <summary>
	/// Async connection proxy class.
	/// </summary>
	public class AsyncConnectionProxyStream : IAsyncConnection
	{
		private static readonly String NotSupported = "Method not supported in proxy client: ";
		AsyncServerStreamingCall<AerospikeResponsePayload> Stream;
		AerospikeResponsePayload Response;
		byte[] Payload;
		int Offset;
		int BufferOffset;

		public AsyncConnectionProxyStream(AsyncServerStreamingCall<AerospikeResponsePayload> stream, IAsyncCommand command)
		{
			Command = command;
			Stream = stream;
			stream.ResponseStream.MoveNext().Wait();
			Response = stream.ResponseStream.Current;
			if (Response.Status != 0)
			{
				throw GRPCConversions.GrpcStatusError(Response);
			}
			Payload = Response.Payload.ToByteArray();
			Offset = 0;
			if (!Response.HasNext)
			{
				throw new EndOfGRPCStream();
			}
		}

		public IAsyncCommand Command
		{
			get;
			set;
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

		void IAsyncConnection.Receive(byte[] buffer, int offset, int count)
		{
			throw new NotImplementedException();
		}

		public async Task Receive(byte[] buffer, int offset, int count)
		{
			BufferOffset = 0;
			await GRPCRead(buffer, count);
			await Command.ReceiveComplete(this);
		}

		public async Task GRPCRead(byte[] buffer, int length)
		{
			if (Payload == null)
			{
				await NextGRPCResponse();
			}

			if (length > Payload.Length - Offset)
			{
				// Copy remaining data
				Array.Copy(Payload, Offset, buffer, BufferOffset, Payload.Length - Offset);
				BufferOffset += Payload.Length - Offset;
				// Reset payload
				Payload = null;
				// Get the next response
				await GRPCRead(buffer, length);
			}

			Array.Copy(Payload, Offset, buffer, BufferOffset, length);
			Offset += length;

			if (Offset >= Payload.Length)
			{
				Payload = null;
			}
		}

		private async Task NextGRPCResponse()
		{
			await Stream.ResponseStream.MoveNext();
			Response = Stream.ResponseStream.Current;
			if (Response.Status != 0)
			{
				throw GRPCConversions.GrpcStatusError(Response);
			}
			Payload = Response.Payload.ToByteArray();
			Offset = 0;
			if (!Response.HasNext)
			{
				throw new EndOfGRPCStream();
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
