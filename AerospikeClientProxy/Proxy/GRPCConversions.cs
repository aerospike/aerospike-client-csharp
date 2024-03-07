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
using Google.Protobuf;
using Grpc.Core;

namespace Aerospike.Client
{
	internal class GRPCConversions
	{
		private static readonly string ERROR_MESSAGE_SEPARATOR = " -> ";
		public static readonly int MAX_ERR_MSG_LENGTH = 10 * 1024;

		public static void SetRequestPolicy(
			Policy policy,
			KVS.AerospikeRequestPayload request
		)
		{
			if (policy.GetType() == typeof(WritePolicy))
			{
				KVS.WritePolicy writePolicy = new()
				{
					ReadModeAP = ToGrpc(policy.readModeAP),
					ReadModeSC = ToGrpc(policy.readModeSC),
					Replica = ToGrpc(policy.replica)
				};

				request.WritePolicy = writePolicy;
			}
			else
			{
				KVS.ReadPolicy readPolicy = new()
				{
					ReadModeAP = ToGrpc(policy.readModeAP),
					ReadModeSC = ToGrpc(policy.readModeSC),
					Replica = ToGrpc(policy.replica)
				};

				request.ReadPolicy = readPolicy;
			}
		}

		public static KVS.ScanPolicy ToGrpc(ScanPolicy scanPolicy)
		{
			KVS.ScanPolicy scanPolicyKVS = new()
			{
				// Base policy fields.
				ReadModeAP = ToGrpc(scanPolicy.readModeAP),
				ReadModeSC = ToGrpc(scanPolicy.readModeSC),
				Replica = ToGrpc(scanPolicy.replica),
				TotalTimeout = (uint)scanPolicy.totalTimeout,
				Compress = scanPolicy.compress,
				// Scan policy specific fields
				MaxRecords = (ulong)scanPolicy.maxRecords,
				RecordsPerSecond = (uint)scanPolicy.recordsPerSecond,
				MaxConcurrentNodes = (uint)scanPolicy.maxConcurrentNodes,
				ConcurrentNodes = scanPolicy.concurrentNodes,
				IncludeBinData = scanPolicy.includeBinData
			};
			if (scanPolicy.filterExp != null) { scanPolicyKVS.Expression = ByteString.CopyFrom(scanPolicy.filterExp.Bytes); }

			return scanPolicyKVS;
		}

		public static KVS.QueryPolicy ToGrpc(QueryPolicy queryPolicy)
		{
			var queryPolicyKVS = new KVS.QueryPolicy()
			{
				// Base policy fields.
				ReadModeAP = ToGrpc(queryPolicy.readModeAP),
				ReadModeSC = ToGrpc(queryPolicy.readModeSC),
				Replica = ToGrpc(queryPolicy.replica),
				TotalTimeout = (uint)queryPolicy.totalTimeout,
				Compress = queryPolicy.compress,
				SendKey = queryPolicy.sendKey,
				// Query policy specific fields
				MaxConcurrentNodes = (uint)queryPolicy.maxConcurrentNodes,
				RecordQueueSize = (uint)queryPolicy.recordQueueSize,
				InfoTimeout = queryPolicy.infoTimeout,
				IncludeBinData = queryPolicy.includeBinData,
				FailOnClusterChange = queryPolicy.failOnClusterChange,
				ShortQuery = queryPolicy.shortQuery
			};
			if (queryPolicy.filterExp != null) {
				queryPolicyKVS.Expression = ByteString.CopyFrom(queryPolicy.filterExp.Bytes);
			}
			return queryPolicyKVS;
		}

		/**
		 * Convert a value to packed bytes.
		 *
		 * @param value the value to pack
		 * @return the packed bytes.
		 */
		public static ByteString ValueToByteString(Value value)
		{
			Packer packer = new();
			value.Pack(packer);
			return ByteString.CopyFrom(packer.ToByteArray());
		}

		public static KVS.Filter ToGrpc(Filter filter)
		{
			Packer packer = new();
			filter.Begin?.Pack(packer);

			var filterKVS = new KVS.Filter()
			{
				Name = filter.Name,
				ValType = filter.ValType,
				Begin = filter.Begin == null ? null : ByteString.CopyFrom(packer.ToByteArray()),
				End = filter.End == null ? null : ValueToByteString(filter.End),
				ColType = ToGrpc(filter.ColType)
			};

			if (filter.PackedCtx != null) {
				filterKVS.PackedCtx = ByteString.CopyFrom(filter.PackedCtx);
			}

			return filterKVS;
		}

		public static KVS.Operation ToGrpc(Operation operation)
		{
			var operationKVS = new KVS.Operation()
			{
				Type = ToGrpc(operation.type),
				Value = ValueToByteString(operation.value)
			};
			if (operation.binName != null) 
			{ 
				operationKVS.BinName = operation.binName; 
			}

			return operationKVS;
		}

		/**
		 * @param statement 	Aerospike client statement
		 * @param taskId    	required non-zero taskId to use for the execution at the proxy
		 * @param maxRecords	max records to return
		 * @return equivalent gRPC {@link com.aerospike.proxy.client.KVS.Statement}
		 */
		public static KVS.Statement ToGrpc(Statement statement, long taskId, long maxRecords)
		{
			var statementKVS = new KVS.Statement
			{
				Namespace = statement.Namespace,
				SetName = statement.SetName ?? "",
				Filter = statement.Filter != null ? ToGrpc(statement.Filter) : null,
				TaskId = taskId,
				MaxRecords = (ulong)maxRecords,
				RecordsPerSecond = (uint)statement.RecordsPerSecond
			};

			if (statement.IndexName != null)
			{
				statementKVS.IndexName = statement.IndexName;
			}
			if (statement.PackageName != null) 
			{
				statementKVS.PackageName = statement.PackageName;
			}
			if (statement.FunctionName != null) 
			{
				statementKVS.FunctionName = statement.FunctionName;
			}


			if (statement.BinNames != null)
			{
				foreach (string binName in statement.BinNames)
				{
					statementKVS.BinNames.Add(binName);
				}
			}

			if (statement.FunctionArgs != null)
			{
				foreach (Value arg in statement.FunctionArgs)
				{
					statementKVS.FunctionArgs.Add(ValueToByteString(arg));
				}
			}

			if (statement.Operations != null)
			{
				foreach (Operation operation in statement.Operations)
				{
					statementKVS.Operations.Add(ToGrpc(operation));
				}
			}

			return statementKVS;
		}

		public static KVS.PartitionStatus ToGrpc(PartitionStatus ps)
		{
			var partitionStatusKVS = new KVS.PartitionStatus()
			{
				Id = (uint)ps.id,
				BVal = (long)ps.bval,
				Retry = ps.retry
			};

			if (ps.digest != null) 
			{ 
				partitionStatusKVS.Digest = ByteString.CopyFrom(ps.digest); 
			}

			return partitionStatusKVS;
		}

		public static KVS.PartitionFilter ToGrpc(PartitionFilter partitionFilter)
		{
			var partitionFilterKVS = new KVS.PartitionFilter()
			{
				Begin = (uint)partitionFilter.Begin,
				Count = (uint)partitionFilter.Count,
				Retry = partitionFilter.Retry
			};

			if (partitionFilter.Digest != null && partitionFilter.Digest.Length > 0) 
			{
				partitionFilterKVS.Digest = ByteString.CopyFrom(partitionFilter.Digest);
			}

			if (partitionFilter.Partitions != null)
			{
				foreach (PartitionStatus partition in partitionFilter.Partitions)
				{
					partitionFilterKVS.PartitionStatuses.Add(ToGrpc(partition));
				}
			}

			return partitionFilterKVS;
		}

		public static KVS.BackgroundExecutePolicy ToGrpcExec(WritePolicy writePolicy)
		{
			return new()
			{
				// Base policy fields.
				ReadModeAP = ToGrpc(writePolicy.readModeAP),
				ReadModeSC = ToGrpc(writePolicy.readModeSC),
				Replica = ToGrpc(writePolicy.replica),
				Expression = writePolicy.filterExp == null ? ByteString.Empty : ByteString.CopyFrom(writePolicy.filterExp.Bytes),
				TotalTimeout = (uint)writePolicy.totalTimeout,
				Compress = writePolicy.compress,
				SendKey = writePolicy.sendKey,
				// Query policy specific fields
				RecordExistsAction = ToGrpc(writePolicy.recordExistsAction),
				GenerationPolicy = ToGrpc(writePolicy.generationPolicy),
				CommitLevel = ToGrpc(writePolicy.commitLevel),
				Generation = (uint)writePolicy.generation,
				Expiration = (uint)writePolicy.expiration,
				RespondAllOps = writePolicy.respondAllOps,
				DurableDelete = writePolicy.durableDelete,
				//Xdr = writePolicy.Xdr
			};
		}

		public static KVS.WritePolicy ToGrpc(WritePolicy writePolicy)
		{
			return new()
			{
				// Base policy fields.
				ReadModeAP = ToGrpc(writePolicy.readModeAP),
				ReadModeSC = ToGrpc(writePolicy.readModeSC),
				Replica = ToGrpc(writePolicy.replica),
			};
		}

		private static KVS.ReadModeAP ToGrpc(ReadModeAP readModeAP)
		{
			return readModeAP switch
			{
				ReadModeAP.ONE => KVS.ReadModeAP.One,
				ReadModeAP.ALL => KVS.ReadModeAP.All,
				_ => KVS.ReadModeAP.One,
			};
		}

		private static KVS.ReadModeSC ToGrpc(ReadModeSC readModeSC)
		{
			return readModeSC switch
			{
				ReadModeSC.SESSION => KVS.ReadModeSC.Session,
				ReadModeSC.LINEARIZE => KVS.ReadModeSC.Linearize,
				ReadModeSC.ALLOW_REPLICA => KVS.ReadModeSC.AllowReplica,
				ReadModeSC.ALLOW_UNAVAILABLE => KVS.ReadModeSC.AllowUnavailable,
				_ => KVS.ReadModeSC.Session
			};
		}

		private static KVS.Replica ToGrpc(Replica replica)
		{
			return replica switch
			{
				Replica.MASTER => KVS.Replica.Master,
				Replica.MASTER_PROLES => KVS.Replica.MasterProles,
				Replica.SEQUENCE => KVS.Replica.Sequence,
				Replica.PREFER_RACK => KVS.Replica.PreferRack,
				Replica.RANDOM => KVS.Replica.Random,
				_ => KVS.Replica.Master
			};
		}

		private static KVS.IndexCollectionType ToGrpc(IndexCollectionType indexCollectionType)
		{
			return indexCollectionType switch
			{
				IndexCollectionType.DEFAULT => KVS.IndexCollectionType.Default,
				IndexCollectionType.LIST => KVS.IndexCollectionType.List,
				IndexCollectionType.MAPKEYS => KVS.IndexCollectionType.Mapkeys,
				IndexCollectionType.MAPVALUES => KVS.IndexCollectionType.Mapvalues,
				_ => KVS.IndexCollectionType.Default
			};
		}

		private static KVS.OperationType ToGrpc(Operation.Type operationType)
		{
			return operationType switch
			{
				Operation.Type.READ => KVS.OperationType.Read,
				Operation.Type.READ_HEADER => KVS.OperationType.ReadHeader,
				Operation.Type.WRITE => KVS.OperationType.Write,
				Operation.Type.CDT_READ => KVS.OperationType.CdtRead,
				Operation.Type.CDT_MODIFY => KVS.OperationType.CdtModify,
				Operation.Type.MAP_READ => KVS.OperationType.MapRead,
				Operation.Type.MAP_MODIFY => KVS.OperationType.MapModify,
				Operation.Type.ADD => KVS.OperationType.Add,
				Operation.Type.EXP_READ => KVS.OperationType.ExpRead,
				Operation.Type.EXP_MODIFY => KVS.OperationType.ExpModify,
				Operation.Type.APPEND => KVS.OperationType.Append,
				Operation.Type.PREPEND => KVS.OperationType.Prepend,
				Operation.Type.TOUCH => KVS.OperationType.Touch,
				Operation.Type.BIT_READ => KVS.OperationType.BitRead,
				Operation.Type.BIT_MODIFY => KVS.OperationType.BitModify,
				Operation.Type.DELETE => KVS.OperationType.Delete,
				Operation.Type.HLL_READ => KVS.OperationType.HllRead,
				Operation.Type.HLL_MODIFY => KVS.OperationType.HllModify,
				_ => KVS.OperationType.Add
			};
		}

		private static KVS.RecordExistsAction ToGrpc(RecordExistsAction recordExistsAction)
		{
			return recordExistsAction switch
			{
				RecordExistsAction.UPDATE => KVS.RecordExistsAction.Update,
				RecordExistsAction.UPDATE_ONLY => KVS.RecordExistsAction.UpdateOnly,
				RecordExistsAction.REPLACE => KVS.RecordExistsAction.Replace,
				RecordExistsAction.REPLACE_ONLY => KVS.RecordExistsAction.ReplaceOnly,
				RecordExistsAction.CREATE_ONLY => KVS.RecordExistsAction.CreateOnly,
				_ => KVS.RecordExistsAction.Update
			};
		}

		private static KVS.GenerationPolicy ToGrpc(GenerationPolicy generationPolicy)
		{
			return generationPolicy switch
			{
				GenerationPolicy.NONE => KVS.GenerationPolicy.None,
				GenerationPolicy.EXPECT_GEN_EQUAL => KVS.GenerationPolicy.ExpectGenEqual,
				GenerationPolicy.EXPECT_GEN_GT => KVS.GenerationPolicy.ExpectGenGt,
				_ => KVS.GenerationPolicy.None
			};
		}

		private static KVS.CommitLevel ToGrpc(CommitLevel commitLevel)
		{
			return commitLevel switch
			{
				CommitLevel.COMMIT_ALL => KVS.CommitLevel.CommitAll,
				CommitLevel.COMMIT_MASTER => KVS.CommitLevel.CommitMaster,
				_ => KVS.CommitLevel.CommitAll
			};
		}

		public static AerospikeException ToAerospikeException(RpcException rpc, int timeout, bool inDoubt)
		{
			StatusCode code = rpc.StatusCode;
			int resultCode = ResultCode.CLIENT_ERROR;
			switch (code)
			{
				case StatusCode.Cancelled:
				case StatusCode.Unknown:
				case StatusCode.NotFound:
				case StatusCode.AlreadyExists:
				case StatusCode.FailedPrecondition:
				case StatusCode.OutOfRange:
				case StatusCode.Unimplemented:
				case StatusCode.Internal:
					resultCode = ResultCode.CLIENT_ERROR;
					break;

				case StatusCode.Aborted:
				case StatusCode.DataLoss:
					resultCode = ResultCode.SERVER_ERROR;
					break;

				case StatusCode.InvalidArgument:
					resultCode = ResultCode.SERIALIZE_ERROR;
					break;

				case StatusCode.DeadlineExceeded:
					if (Log.DebugEnabled())
					{
						Log.Debug($"AerospikeException Deadline: {resultCode}: Exception: {rpc.GetType()} Message: '{rpc.Message}': '{rpc}'");
					}

					return new AerospikeException.Timeout(timeout, inDoubt, rpc);

				case StatusCode.PermissionDenied:
					resultCode = ResultCode.FAIL_FORBIDDEN;
					break;

				case StatusCode.ResourceExhausted:
					resultCode = ResultCode.QUOTA_EXCEEDED;
					break;

				case StatusCode.Unauthenticated:
					resultCode = ResultCode.NOT_AUTHENTICATED;
					break;

				case StatusCode.Unavailable:
					resultCode = ResultCode.SERVER_NOT_AVAILABLE;
					break;

				case StatusCode.OK:
					resultCode = ResultCode.OK;
					break;
			}

			if (Log.DebugEnabled())
			{
				Log.Debug($"AerospikeException: {resultCode}: Exception: {rpc.GetType()} Message: '{rpc.Message}': '{rpc}'");
			}

			return new AerospikeException(resultCode, GetDisplayMessage(rpc, MAX_ERR_MSG_LENGTH), rpc);
		}

		/**
		 * Get the error message to display restricting it to some length.
		 */
		public static string GetDisplayMessage(Exception e, int maxMsgLength)
		{
			if (maxMsgLength <= 0)
			{
				return "";
			}

			string errorMessage = GetMessage(e);
			Exception rootCause = e.InnerException;
			while (rootCause != null)
			{
				string current = GetMessage(rootCause);
				errorMessage = String.IsNullOrEmpty(errorMessage) ? current
					: errorMessage + ERROR_MESSAGE_SEPARATOR + current;
				rootCause = rootCause.InnerException;
			}

			return Take(errorMessage, maxMsgLength);
		}

		public static AerospikeException GrpcStatusError(AerospikeResponsePayload response)
		{
			if (response.Status >= 0)
			{
				return new AerospikeException(response.Status).SetInDoubt(response.InDoubt);
			}

			if (response.Status == -9) return new AerospikeException(ResultCode.SERVER_ERROR, "Server ASYNC_QUEUE_FULL").SetInDoubt(response.InDoubt);

			return new AerospikeException(response.Status).SetInDoubt(response.InDoubt);
		}

		/**
		 * Take at most first `n` characters from the string.
		 *
		 * @param s input string
		 * @param n number of characters to take.
		 * @return the string that is at most `n` characters in length.
		 */
		private static String Take(String s, int n)
		{
			int trimLength = Math.Min(n, s.Length);
			if (trimLength <= 0)
			{
				return "";
			}
			return s[..trimLength];
		}

		/**
		 * Get error message for [e].
		 */
		private static string GetMessage(Exception e)
		{
			if (e == null)
			{
				return "";
			}

			string errorMessage = e.Message ?? "";

			errorMessage = errorMessage.Split("\\r?\\n|\\r")[0];
			return errorMessage;
		}
	}
}
