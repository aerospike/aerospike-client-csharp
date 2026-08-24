/* 
 * Copyright 2012-2026 Aerospike, Inc.
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
namespace Aerospike.Client
{
	/// <summary>
	/// Batch key and record result.
	/// </summary>
	public class BatchRecord
	{
		/// <summary>
		/// Key.
		/// </summary>
		public readonly Key key;

		/// <summary>
		/// Record result after batch command has completed.  Will be null if record was not found
		/// or an error occurred. See <see cref="BatchRecord.resultCode"/>.
		/// </summary>
		public Record record;

		/// <summary>
		/// Result code for this returned record. See <see cref="Aerospike.Client.ResultCode"/>.
		/// If not <see cref="Aerospike.Client.ResultCode.OK"/>, the record will be null.
		/// Key-specific errors can be reported here even when other records in the same batch succeed.
		/// </summary>
		public int resultCode;

		/// <summary>
		/// Is it possible that the write command may have completed even though an error
		/// occurred for this record. This may be the case when a client error occurs (like timeout)
		/// after the command was sent to the server.
		/// This field is meaningful for batch records that contain writes; applications should
		/// resolve an in-doubt result before blindly retrying a non-idempotent operation.
		/// </summary>
		public bool inDoubt;

		/// <summary>
		/// Formatted server-side error detail (human-readable message and/or sub-code) for this
		/// record, populated when the batch opted into
		/// <see cref="Policy.errorDetailVerbosity"/> greater than zero and the server attached
		/// an extended error detail. <c>null</c> otherwise.
		/// Requires server version 8.1.3+.
		/// </summary>
		public string serverMessage;

		/// <summary>
		/// Server-supplied error sub-code for this record, or <see cref="SubCode.NONE"/> when
		/// the server did not send one. A sub-code is only meaningful when interpreted together
		/// with <see cref="resultCode"/>: sub-code integer values are scoped to their parent
		/// result code and are not globally unique. See <see cref="SubCode"/>.
		/// </summary>
		public int subCode = SubCode.NONE;

		/// <summary>
		/// Server-supplied expression trace for this record, sent only at
		/// <see cref="Policy.errorDetailVerbosity"/> level 3 on expression failure
		/// paths. <c>null</c> when absent. See <see cref="ExpressionTrace"/>.
		/// </summary>
		public ExpressionTrace expTrace;

		/// <summary>
		/// Does this command contain a write operation. For internal use only.
		/// </summary>
		public readonly bool hasWrite;

		/// <summary>
		/// Initialize batch key.
		/// </summary>
		public BatchRecord(Key key, bool hasWrite)
		{
			this.key = key;
			this.resultCode = ResultCode.NO_RESPONSE;
			this.hasWrite = hasWrite;
		}

		/// <summary>
		/// Initialize batch key and record.
		/// </summary>
		public BatchRecord(Key key, Record record, bool hasWrite)
		{
			this.key = key;
			this.record = record;
			this.resultCode = ResultCode.OK;
			this.hasWrite = hasWrite;
		}

		/// <summary>
		/// Error constructor.
		/// </summary>
		public BatchRecord(Key key, Record record, int resultCode, bool inDoubt, bool hasWrite)
		{
			this.key = key;
			this.record = record;
			this.resultCode = resultCode;
			this.inDoubt = inDoubt;
			this.hasWrite = hasWrite;
		}

		/// <summary>
		/// Prepare for upcoming batch call. Reset result fields because this instance might be
		/// reused. For internal use only.
		/// </summary>
		public void Prepare()
		{
			this.record = null;
			this.resultCode = ResultCode.NO_RESPONSE;
			this.inDoubt = false;
			this.serverMessage = null;
			this.subCode = SubCode.NONE;
			this.expTrace = null;
		}

		/// <summary>
		/// Set the server-supplied error detail (extended error) for this record.
		/// For internal use only.
		/// </summary>
		internal void SetErrorDetail(string serverMessage, int subCode, ExpressionTrace expTrace)
		{
			this.serverMessage = serverMessage;
			this.subCode = subCode;
			this.expTrace = expTrace;
		}

		/// <summary>
		/// Set record result. For internal use only.
		/// </summary>
		public void SetRecord(Record record)
		{
			this.record = record;
			this.resultCode = ResultCode.OK;
		}

		/// <summary>
		/// Set error result. For internal use only.
		/// </summary>
		public void SetError(int resultCode, bool inDoubt)
		{
			this.resultCode = resultCode;
			this.inDoubt = inDoubt;
		}

		/// <summary>
		/// Convert to string.
		/// </summary>
		public override string ToString()
		{
			return key.ToString();
		}

		/// <summary>
		/// Return batch command type. For internal use only.
		/// </summary>
		public virtual Type GetBatchType()
		{
			// This method should always be overriden.
			throw new AerospikeException("Invalid GetBatchType call");
		}

		public virtual bool ResolveSendKey(
			Policy parentPolicy,
			IConfigProvider configProvider,
			BatchWritePolicy writePolicyDefault,
			BatchUDFPolicy udfPolicyDefault,
			BatchDeletePolicy deletePolicyDefault
		)
		{
			return false;
		}

		/// <summary>
		/// Optimized reference equality check to determine batch wire protocol repeat flag.
		/// For internal use only.
		/// </summary>
		public virtual bool Equals(BatchRecord other)
		{
			return false;
		}

		/// <summary>
		/// Return wire protocol size. For internal use only.
		/// </summary>
		public virtual int Size(bool sendKey)
		{
			return 0;
		}

		/// <summary>
		/// Batch command type.
		/// </summary>
		public enum Type
		{
			BATCH_READ,
			BATCH_WRITE,
			BATCH_DELETE,
			BATCH_UDF
		}
	}
}
