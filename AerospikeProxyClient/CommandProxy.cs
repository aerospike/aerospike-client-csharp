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
using Aerospike.Client.Proxy.KVS;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Net.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Channels;

#pragma warning disable 0618

namespace Aerospike.Client.Proxy
{
	public class CommandProxy : Command
	{
		int generation;
		int expiration;
		int batchIndex;
		int fieldCount;
		int opCount;

		public CommandProxy(Policy policy)
			: base(policy.socketTimeout, policy.totalTimeout, policy.maxRetries)
		{
		}

		public void Write(GrpcChannel channel, WritePolicy writePolicy, Key key, Bin[] bins, Operation.Type operation)
		{
			SetWrite(writePolicy, Operation.Type.WRITE, key, bins);
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(writePolicy, request);

			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Write(request);
			UnloadResponse(response);
			ParseResult(writePolicy);
		}

		public async Task WriteAsync(GrpcChannel channel, WritePolicy writePolicy, CancellationToken token, Key key, Bin[] bins, Operation.Type operation)
		{
			SetWrite(writePolicy, Operation.Type.WRITE, key, bins);
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(writePolicy, request);

			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.WriteAsync(request, cancellationToken: token);
			UnloadResponse(response);
			ParseResult(writePolicy);
		}

		public Record Read(GrpcChannel channel, Policy policy, Key key)
		{
			string[] binNames = null;
			SetRead(policy, key, binNames);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Read(request);
			UnloadResponse(response);
			return ParseRecordResult(policy);
		}

		public async Task<Record> ReadAsync(GrpcChannel channel, Policy policy, Key key, CancellationToken token)
		{
			string[] binNames = null;
			SetRead(policy, key, binNames);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.ReadAsync(request, cancellationToken: token);
			UnloadResponse(response);
			return ParseRecordResult(policy);
		}

		public Record ReadHeader(GrpcChannel channel, Policy policy, Key key)
		{
			SetReadHeader(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Read(request);
			UnloadResponse(response);
			return ParseResultHeader(policy);
		}

		public async Task<Record> ReadHeaderAsync(GrpcChannel channel, Policy policy, Key key, CancellationToken token)
		{
			SetReadHeader(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.ReadAsync(request, cancellationToken: token);
			UnloadResponse(response);
			return ParseResultHeader(policy);
		}

		public bool Delete(GrpcChannel channel, WritePolicy policy, Key key)
		{
			SetDelete(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Delete(request);
			UnloadResponse(response);
			return ParseResultDelete(policy);
		}

		public async Task<bool> DeleteAsync(GrpcChannel channel, WritePolicy policy, Key key, CancellationToken token)
		{
			SetDelete(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.DeleteAsync(request, cancellationToken: token);
			UnloadResponse(response);
			return ParseResultDelete(policy);
		}

		public void Touch(GrpcChannel channel, WritePolicy policy, Key key)
		{
			SetTouch(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Touch(request);
			UnloadResponse(response);
			ParseResultTouch(policy);
		}

		public async Task TouchAsync(GrpcChannel channel, WritePolicy policy, Key key, CancellationToken token)
		{
			SetTouch(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.TouchAsync(request, cancellationToken: token);
			UnloadResponse(response);
			ParseResultTouch(policy);
		}

		public bool Exists(GrpcChannel channel, Policy policy, Key key)
		{
			SetExists(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Exists(request);
			UnloadResponse(response);
			return ParseResultExists(policy);
		}

		public async Task<bool> ExistsAsync(GrpcChannel channel, Policy policy, Key key, CancellationToken token)
		{
			SetExists(policy, key);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.ExistsAsync(request, cancellationToken: token);
			UnloadResponse(response);
			return ParseResultExists(policy);
		}

		public Record Operate(GrpcChannel channel, Key key, OperateArgs args)
		{
			SetOperate(args.writePolicy, key, args);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(args.writePolicy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Operate(request);
			UnloadResponse(response);
			return ParseRecordResult(args.writePolicy);
		}

		public async Task<Record> OperateAsync(GrpcChannel channel, Key key, OperateArgs args, CancellationToken token)
		{
			SetOperate(args.writePolicy, key, args);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(args.writePolicy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.OperateAsync(request, cancellationToken: token);
			UnloadResponse(response);
			return ParseRecordResult(args.writePolicy);
		}

		public Record Execute(GrpcChannel channel, WritePolicy policy, Key key, string packageName, string functionName, params Value[] args)
		{
			SetUdf(policy, key, packageName, functionName, args);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Execute(request);
			UnloadResponse(response);
			return ParseRecordResult(policy);
		}

		public async Task<Record> ExecuteAsync(GrpcChannel channel, CancellationToken token, WritePolicy policy, Key key, string packageName, string functionName, params Value[] args)
		{
			SetUdf(policy, key, packageName, functionName, args);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.ExecuteAsync(request, cancellationToken: token);
			UnloadResponse(response);
			return ParseRecordResult(policy);
		}

		private void UnloadResponse(AerospikeResponsePayload response)
		{
			dataBuffer = response.Payload.ToByteArray();
			dataOffset = 13;
		}

		internal void ParseResult(WritePolicy policy)
		{
			int resultCode = ParseResultCode();

			if (resultCode == 0)
			{
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}

		internal Record ParseResultHeader(Policy policy)
		{
			int resultCode = ParseResultCode();

			if (resultCode == 0)
			{
				int generation = ByteUtil.BytesToInt(dataBuffer, 14);
				int expiration = ByteUtil.BytesToInt(dataBuffer, 18);
				return new Record(null, generation, expiration);
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return null;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return null;
			}

			throw new AerospikeException(resultCode);
			return null;
		}

		internal Record ParseRecordResult(Policy policy)
		{
			Record record = null;
			int resultCode = ParseHeader();

			switch (resultCode)
			{
				case ResultCode.OK:
					SkipKey();
					if (opCount == 0)
					{
						// Bin data was not returned.
						record = new Record(null, generation, expiration);
					}
					else
					{
						record = ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, false);
					}
					break;

				case ResultCode.KEY_NOT_FOUND_ERROR:
					//handleNotFound(resultCode);
					break;

				case ResultCode.FILTERED_OUT:
					if (policy.failOnFilteredOut)
					{
						throw new AerospikeException(resultCode);
					}
					break;

				case ResultCode.UDF_BAD_RESPONSE:
					SkipKey();
					record = ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, false);
					//handleUdfError(record, resultCode);
					break;

				default:
					throw new AerospikeException(resultCode);
			}

			return record;
		}

		internal bool ParseResultDelete(WritePolicy policy)
		{
			int resultCode = ParseResultCode();

			if (resultCode == 0)
			{
				return true;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return false;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return true;
			}

			throw new AerospikeException(resultCode);
			return false;
		}

		internal void ParseResultTouch(WritePolicy writePolicy)
		{
			int resultCode = ParseResultCode();

			if (resultCode == 0)
			{
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (writePolicy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}

		internal bool ParseResultExists(Policy policy)
		{
			int resultCode = ParseResultCode();

			if (resultCode == 0)
			{
				return true;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return false;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return true;
			}

			throw new AerospikeException(resultCode);
			return false;
		}

		public int ParseResultCode()
		{
			return dataBuffer[13] & 0xFF;
		}

		public int ParseHeader()
		{
			var resultCode = ParseResultCode();
			dataOffset += 1;
			generation = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			batchIndex = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			return resultCode;
		}

		public Record ParseRecord(byte[] dataBuffer, ref int dataOffsetRef, int opCount, int generation, int expiration, bool isOperation)
		{
			Dictionary<String, Object> bins = new Dictionary<string, object>();
			int dataOffset = dataOffsetRef;

			for (int i = 0; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				byte particleType = dataBuffer[dataOffset + 5];
				byte nameSize = dataBuffer[dataOffset + 7];
				String name = ByteUtil.Utf8ToString(dataBuffer, dataOffset + 8, nameSize);
				dataOffset += 4 + 4 + nameSize;

				int particleBytesSize = opSize - (4 + nameSize);
				Object value = ByteUtil.BytesToParticle((ParticleType)particleType, dataBuffer, dataOffset, particleBytesSize);
				dataOffset += particleBytesSize;

				if (isOperation)
				{
					object prev;

					if (bins.TryGetValue(name, out prev))
					{
						// Multiple values returned for the same bin. 
						if (prev is RecordParser.OpResults)
						{
							// List already exists.  Add to it.
							RecordParser.OpResults list = (RecordParser.OpResults)prev;
							list.Add(value);
						}
						else
						{
							// Make a list to store all values.
							RecordParser.OpResults list = new RecordParser.OpResults();
							list.Add(prev);
							list.Add(value);
							bins[name] = list;
						}
					}
					else
					{
						bins[name] = value;
					}
				}
				else
				{
					bins[name] = value;
				}
			}
			dataOffsetRef = dataOffset;
			return new Record(bins, generation, expiration);
		}

		public void SkipKey()
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldlen;
			}
		}

		protected override int SizeBuffer()
		{
			dataBuffer = ThreadLocalData.GetBuffer();

			if (dataOffset > dataBuffer.Length)
			{
				dataBuffer = ThreadLocalData.ResizeBuffer(dataOffset);
			}
			dataOffset = 0;
			return dataBuffer.Length;
		}

		protected override void End()
		{
			// Write total size of message.
			ulong size = ((ulong)dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);
		}

		protected override void SetLength(int length)
		{
			dataOffset = length;
		}

		
	}
}
#pragma warning restore 0618
