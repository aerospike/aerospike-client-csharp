/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	/// <summary>
	/// Connection wrapper for GRPC response stream.
	/// </summary>
	public class ConnectionProxyStream : IConnection
	{
		private static readonly String NotSupported = "Method not supported in proxy client: ";
		private AsyncServerStreamingCall<AerospikeResponsePayload> Stream { get; }
		private AerospikeResponsePayload Response { get; set; }
		private byte[] Payload { get; set; }
		private int Offset { get; set; }
		private int BufferOffset { get; set; }

		/// <summary>
		/// Create GRPC Connection Stream class
		/// </summary>
		public ConnectionProxyStream(AsyncServerStreamingCall<AerospikeResponsePayload> stream)
		{
			Stream = stream ?? throw new ArgumentNullException(nameof(stream));
			Payload = null;
		}

		public void SetTimeout(int timeoutMillis)
		{
			throw new AerospikeException(NotSupported + "SetTimeout");
		}

		public void Write(byte[] buffer, int length)
		{
			throw new AerospikeException(NotSupported + "Write");
		}

		void IConnection.ReadFully(byte[] buffer, int length)
		{
			throw new NotImplementedException();
		}

		public async Task ReadFully(byte[] buffer, int length, CancellationToken token)
		{
			BufferOffset = 0;
			await GRPCRead(buffer, length, token);
		}

		public async Task GRPCRead(byte[] buffer, int length, CancellationToken token)
		{
			if (Payload == null)
			{
				await NextGRPCResponse(token);
			}

			token.ThrowIfCancellationRequested();

			if (length > Payload.Length - Offset)
			{
				// Copy remaining data
				Array.Copy(Payload, Offset, buffer, BufferOffset, Payload.Length - Offset);
				BufferOffset += Payload.Length - Offset;
				// Reset payload
				Payload = null;
				// Get the next response
				await GRPCRead(buffer, length, token);
			}

			Array.Copy(Payload, Offset, buffer, BufferOffset, length);
			Offset += length;

			if (Offset >= Payload.Length)
			{
				Payload = null;
			}
		}

		private async Task NextGRPCResponse(CancellationToken token)
		{
			token.ThrowIfCancellationRequested();
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
				throw new EndOfGRPCStream(Payload[13]);
			}
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
