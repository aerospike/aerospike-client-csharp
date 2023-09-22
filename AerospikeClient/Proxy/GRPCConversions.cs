﻿using Aerospike.Client;
using Aerospike.Client.KVS;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

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
			if (policy.GetType() == typeof(WritePolicy)) {
				KVS.WritePolicy writePolicy = new()
				{
					ReadModeAP = Enum.TryParse(policy.readModeAP.ToString(), true, out KVS.ReadModeAP apConversion) ? apConversion : KVS.ReadModeAP.One,
					ReadModeSC = Enum.TryParse(policy.readModeSC.ToString(), true, out KVS.ReadModeSC scConversion) ? scConversion : KVS.ReadModeSC.Session,
					Replica = Enum.TryParse(policy.replica.ToString(), true, out KVS.Replica replicaConversion) ? replicaConversion : KVS.Replica.Sequence
				};

				request.WritePolicy = writePolicy;
			}
			else
			{
				KVS.ReadPolicy readPolicy = new()
				{
					ReadModeAP = Enum.TryParse(policy.readModeAP.ToString(), true, out KVS.ReadModeAP apConversion) ? apConversion : KVS.ReadModeAP.One,
					ReadModeSC = Enum.TryParse(policy.readModeSC.ToString(), true, out KVS.ReadModeSC scConversion) ? scConversion : KVS.ReadModeSC.Session,
					Replica = Enum.TryParse(policy.replica.ToString(), true, out KVS.Replica replicaConversion) ? replicaConversion : KVS.Replica.Sequence
				};

				request.ReadPolicy = readPolicy;
			}
		}

		public static KVS.ScanPolicy ToGrpc(ScanPolicy scanPolicy)
		{
			KVS.ScanPolicy scanPolicyKVS = new()
			{
				// Base policy fields.
				ReadModeAP = Enum.TryParse(scanPolicy.readModeAP.ToString(), true, out KVS.ReadModeAP apConversion) ? apConversion : KVS.ReadModeAP.One,
				ReadModeSC = Enum.TryParse(scanPolicy.readModeSC.ToString(), true, out KVS.ReadModeSC scConversion) ? scConversion : KVS.ReadModeSC.Session,
				Replica = Enum.TryParse(scanPolicy.replica.ToString(), true, out KVS.Replica replicaConversion) ? replicaConversion : KVS.Replica.Sequence,
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
			var queryPolicyKVS =  new KVS.QueryPolicy()
			{ 
				// Base policy fields.
				ReadModeAP = Enum.TryParse(queryPolicy.readModeAP.ToString(), true, out KVS.ReadModeAP apConversion) ? apConversion : KVS.ReadModeAP.One,
				ReadModeSC = Enum.TryParse(queryPolicy.readModeSC.ToString(), true, out KVS.ReadModeSC scConversion) ? scConversion : KVS.ReadModeSC.Session,
				Replica = Enum.TryParse(queryPolicy.replica.ToString(), true, out KVS.Replica replicaConversion) ? replicaConversion : KVS.Replica.Sequence,
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
			if (queryPolicy.filterExp != null) queryPolicyKVS.Expression = ByteString.CopyFrom(queryPolicy.filterExp.Bytes);
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
			// TODO: @Brian is there a better way to convert value to bytes?
			// This involves two copies. One when returning bytes Packer
			// and one for the byte string.
			Packer packer = new();
			value.Pack(packer);
			return ByteString.CopyFrom(packer.ToByteArray());
		}

		public static KVS.Filter ToGrpc(Filter filter)
		{
			Packer packer = new();
			if (filter.Begin != null)
			{
				filter.Begin.Pack(packer);
			}

			var filterKVS =  new KVS.Filter()
			{
				Name = filter.Name,
				ValType = filter.ValType,
				Begin = filter.Begin == null ? null : ByteString.CopyFrom(packer.ToByteArray()), // TODO ask Brian about this
				End = filter.End == null ? null : ValueToByteString(filter.End),
				ColType = Enum.TryParse(filter.ColType.ToString(), true, out KVS.IndexCollectionType colTypeConversion) ? colTypeConversion : KVS.IndexCollectionType.Default
			};

			if (filter.PackedCtx != null) filterKVS.PackedCtx = ByteString.CopyFrom(filter.PackedCtx);

			return filterKVS;

			/*if (filter.getBegin() != null)
			{
				// TODO: @Brian is there a better way to convert value to bytes?
				// This involves two copies. One when returning bytes Packer
				// and one for the byte string.
				Packer packer = new();
				filter.getBegin().pack(packer);
				builder.setBegin(ByteString.CopyFrom(packer.toByteArray()));
			}

			if (filter.getBegin() != null)
			{
				builder.setBegin(valueToByteString(filter.getBegin()));
			}*/
		}

		public static KVS.Operation ToGrpc(Operation operation)
		{
			var operationKVS = new KVS.Operation() 
			{ 
				Type = Enum.TryParse(operation.type.ToString(), true, out KVS.OperationType typeConversion) ? typeConversion : KVS.OperationType.Read,
				Value = ValueToByteString(operation.value)
			};
			if (operation.binName != null) { operationKVS.BinName = operation.binName; }

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
				SetName = statement.SetName != null ? statement.SetName : "",
				Filter = statement.Filter != null ? ToGrpc(statement.Filter) : null,
				TaskId = taskId,
				MaxRecords = (ulong)maxRecords,
				RecordsPerSecond = (uint)statement.RecordsPerSecond
			};

			if (statement.IndexName != null) statementKVS.IndexName = statement.IndexName;
			if (statement.PackageName != null) statementKVS.PackageName = statement.PackageName;
			if (statement.FunctionName != null) statementKVS.FunctionName = statement.FunctionName;
			

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

			if (ps.digest != null) { partitionStatusKVS.Digest = ByteString.CopyFrom(ps.digest); }

			return partitionStatusKVS;
		}

		public static KVS.PartitionFilter ToGrpc(PartitionFilter partitionFilter)
		{
			var partitionFilterKVS =  new KVS.PartitionFilter()
			{
				Begin = (uint)partitionFilter.Begin,
				Count = (uint)partitionFilter.Count,
				Retry = partitionFilter.Retry
			};

			if (partitionFilter.Digest != null && partitionFilter.Digest.Length > 0) partitionFilterKVS.Digest = ByteString.CopyFrom(partitionFilter.Digest);

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
				ReadModeAP = Enum.TryParse(writePolicy.readModeAP.ToString(), true, out KVS.ReadModeAP apConversion) ? apConversion : KVS.ReadModeAP.One,
				ReadModeSC = Enum.TryParse(writePolicy.readModeSC.ToString(), true, out KVS.ReadModeSC scConversion) ? scConversion : KVS.ReadModeSC.Session,
				Replica = Enum.TryParse(writePolicy.replica.ToString(), true, out KVS.Replica replicaConversion) ? replicaConversion : KVS.Replica.Sequence,
				Expression = writePolicy.filterExp == null ? ByteString.Empty : ByteString.CopyFrom(writePolicy.filterExp.Bytes),
				TotalTimeout = (uint)writePolicy.totalTimeout,
				Compress = writePolicy.compress,
				SendKey = writePolicy.sendKey,
				// Query policy specific fields
				RecordExistsAction = Enum.TryParse(writePolicy.recordExistsAction.ToString(), true, out KVS.RecordExistsAction reConversion) ? reConversion : KVS.RecordExistsAction.Update,
				GenerationPolicy = Enum.TryParse(writePolicy.generationPolicy.ToString(), true, out KVS.GenerationPolicy gpConversion) ? gpConversion : KVS.GenerationPolicy.None,
				CommitLevel = Enum.TryParse(writePolicy.commitLevel.ToString(), true, out KVS.CommitLevel clConversion) ? clConversion : KVS.CommitLevel.CommitAll,
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
				ReadModeAP = Enum.TryParse(writePolicy.readModeAP.ToString(), true, out KVS.ReadModeAP apConversion) ? apConversion : KVS.ReadModeAP.One,
				ReadModeSC = Enum.TryParse(writePolicy.readModeSC.ToString(), true, out KVS.ReadModeSC scConversion) ? scConversion : KVS.ReadModeSC.Session,
				Replica = Enum.TryParse(writePolicy.replica.ToString(), true, out KVS.Replica replicaConversion) ? replicaConversion : KVS.Replica.Sequence,
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
					return new AerospikeException.Timeout(timeout, inDoubt);

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

			return new AerospikeException(resultCode, GetDisplayMessage(rpc, MAX_ERR_MSG_LENGTH));
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
			/*if (String.IsNullOrEmpty(errorMessage.Trim()))
			{
				return e.getClass().getName();
			}
			else
			{
				return String.Format("%s - %s", e.getClass().getName(),
					errorMessage);
			}*/
			return errorMessage;
		}
	}
}
