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
namespace Aerospike.Client
{
	/// <summary>
	/// Database operation error codes.
	/// </summary>
	public sealed class ResultCode
	{
		/// <summary>
		/// One or more keys failed in a batch.
		/// Value: -16
		/// </summary>
		public const int BATCH_FAILED = -16;

		/// <summary>
		/// No response received from server.
		/// Value: -15
		/// </summary>
		public const int NO_RESPONSE = -15;

		/// <summary>
		/// Max errors limit reached.
		/// Value: -12
		/// </summary>
		public const int MAX_ERROR_RATE = -12;

		/// <summary>
		/// Max retries limit reached.
		/// Value: -11
		/// </summary>
		public const int MAX_RETRIES_EXCEEDED = -11;

		/// <summary>
		/// Client serialization error.
		/// Value: -10
		/// </summary>
		public const int SERIALIZE_ERROR = -10;

		/// <summary>
		/// Server is not accepting requests.
		/// Value: -8
		/// </summary>
		public const int SERVER_NOT_AVAILABLE = -8;

		/// <summary>
		/// Max connections would be exceeded.  There are no more available connections.
		/// Value: -7
		/// </summary>
		public const int NO_MORE_CONNECTIONS = -7;

		/// <summary>
		/// Asynchronous max concurrent database commands have been exceeded and therefore rejected.
		/// Value: -6
		/// </summary>
		public const int COMMAND_REJECTED = -6;

		/// <summary>
		/// Query was terminated by user.
		/// Value: -5
		/// </summary>
		public const int QUERY_TERMINATED = -5;

		/// <summary>
		/// Scan was terminated by user.
		/// Value: -4
		/// </summary>
		public const int SCAN_TERMINATED = -4;

		/// <summary>
		/// Chosen node is not currently active.
		/// Value: -3
		/// </summary>
		public const int INVALID_NODE_ERROR = -3;

		/// <summary>
		/// Client parse error.
		/// Value: -2
		/// </summary>
		public const int PARSE_ERROR = -2;

		/// <summary>
		/// Client generic error.
		/// Value: -1
		/// </summary>
		public const int CLIENT_ERROR = -1;

		/// <summary>
		/// Operation was successful.
		/// Value: 0
		/// </summary>
		public const int OK = 0;

		/// <summary>
		/// Unknown server failure. 
		/// Value: 1
		/// </summary>
		public const int SERVER_ERROR = 1;

		/// <summary>
		/// On retrieving, touching or replacing a record that doesn't exist.
		/// Value: 2
		/// </summary>
		public const int KEY_NOT_FOUND_ERROR = 2;

		/// <summary>
		/// On modifying a record with unexpected generation.
		/// Value: 3
		/// </summary>
		public const int GENERATION_ERROR = 3;

		/// <summary>
		/// Bad parameter(s) were passed in database operation call.
		/// Value: 4
		/// </summary>
		public const int PARAMETER_ERROR = 4;

		/// <summary>
		/// On create-only (write unique) operations on a record that already
		/// exists.
		/// Value: 5
		/// </summary>
		public const int KEY_EXISTS_ERROR = 5;

		/// <summary>
		/// Bin already exists on a create-only operation.
		/// Value: 6
		/// </summary>
		public const int BIN_EXISTS_ERROR = 6;

		/// <summary>
		/// Expected cluster was not received.
		/// Value: 7
		/// </summary>
		public const int CLUSTER_KEY_MISMATCH = 7;

		/// <summary>
		/// Server has run out of memory.
		/// Value: 8
		/// </summary>
		public const int SERVER_MEM_ERROR = 8;

		/// <summary>
		/// Client or server has timed out.
		/// Value: 9
		/// </summary>
		public const int TIMEOUT = 9;

		/// <summary>
		/// Operation not allowed in current configuration.
		/// Value: 10
		/// </summary>
		public const int ALWAYS_FORBIDDEN = 10;

		/// <summary>
		/// Partition unavailable.
		/// Value: 11
		/// </summary>
		public const int PARTITION_UNAVAILABLE = 11;

		/// <summary>
		/// Operation is not supported with configured bin type (single-bin or
		/// multi-bin).
		/// Value: 12
		/// </summary>
		public const int BIN_TYPE_ERROR = 12;

		/// <summary>
		/// Record size exceeds limit.
		/// Value: 13
		/// </summary>
		public const int RECORD_TOO_BIG = 13;

		/// <summary>
		/// Too many concurrent operations on the same record.
		/// Value: 14
		/// </summary>
		public const int KEY_BUSY = 14;

		/// <summary>
		/// Scan aborted by server.
		/// Value: 15
		/// </summary>
		public const int SCAN_ABORT = 15;

		/// <summary>
		/// Client operation not supported on connected server.
		/// Value: 16
		/// </summary>
		public const int UNSUPPORTED_FEATURE = 16;

		/// <summary>
		/// Bin not found on update-only operation.
		/// Value: 17
		/// </summary>
		public const int BIN_NOT_FOUND = 17;

		/// <summary>
		/// Device not keeping up with writes.
		/// Value: 18
		/// </summary>
		public const int DEVICE_OVERLOAD = 18;

		/// <summary>
		/// Key type mismatch.
		/// Value: 19
		/// </summary>
		public const int KEY_MISMATCH = 19;

		/// <summary>
		/// Invalid namespace.
		/// Value: 20
		/// </summary>
		public const int INVALID_NAMESPACE = 20;

		/// <summary>
		/// Bin name length greater than 15 characters or maximum bins exceeded.
		/// Value: 21
		/// </summary>
		public const int BIN_NAME_TOO_LONG = 21;

		/// <summary>
		/// Operation not allowed at this time.
		/// Value: 22
		/// </summary>
		public const int FAIL_FORBIDDEN = 22;

		/// <summary>
		/// Map element not found in UPDATE_ONLY write mode.
		/// Value: 23
		/// </summary>
		public const int ELEMENT_NOT_FOUND = 23;

		/// <summary>
		/// Map element exists in CREATE_ONLY write mode.
		/// Value: 24
		/// </summary>
		public const int ELEMENT_EXISTS = 24;

		/// <summary>
		/// Attempt to use an Enterprise feature on a Community server or a server
		/// without the applicable feature key.
		/// Value: 25
		/// </summary>
		public const int ENTERPRISE_ONLY = 25;

		/// <summary>
		/// The operation cannot be applied to the current bin value on the server.
		/// Value: 26
		/// </summary>
		public const int OP_NOT_APPLICABLE = 26;

		/// <summary>
		/// The transaction was not performed because the filter was false.
		/// Value: 27
		/// </summary>
		public const int FILTERED_OUT = 27;

		/// <summary>
		/// Write command loses conflict to XDR.
		/// Value: 28
		/// </summary>
		public const int LOST_CONFLICT = 28;

		/// <summary>
		/// Write can't complete until XDR finishes shipping.
		/// Value: 32
		/// </summary>
		public const int XDR_KEY_BUSY = 32;

		/// <summary>
		/// There are no more records left for query.
		/// Value: 50
		/// </summary>
		public const int QUERY_END = 50;

		/// <summary>
		/// Security functionality not supported by connected server.
		/// Value: 51
		/// </summary>
		public const int SECURITY_NOT_SUPPORTED = 51;

		/// <summary>
		/// Security functionality supported, but not enabled by connected server.
		/// Value: 52
		/// </summary>
		public const int SECURITY_NOT_ENABLED = 52;

		/// <summary>
		/// Security configuration not supported.
		/// Value: 53
		/// </summary>
		public const int SECURITY_SCHEME_NOT_SUPPORTED = 53;
		
		/// <summary>
		/// Administration command is invalid.
		/// Value: 54
		/// </summary>
		public const int INVALID_COMMAND = 54;

		/// <summary>
		/// Administration field is invalid.
		/// Value: 55
		/// </summary>
		public const int INVALID_FIELD = 55;

		/// <summary>
		/// Server is in illegal stage.
		/// Value: 56
		/// </summary>
		public const int ILLEGAL_STATE = 56;

		/// <summary>
		/// User name is invalid.
		/// Value: 60
		/// </summary>
		public const int INVALID_USER = 60;
		
		/// <summary>
		/// User was previously created.
		/// Value: 61
		/// </summary>
		public const int USER_ALREADY_EXISTS = 61;

		/// <summary>
		/// Password is invalid.
		/// Value: 62
		/// </summary>
		public const int INVALID_PASSWORD = 62;

		/// <summary>
		/// Password has expired.
		/// Value: 63
		/// </summary>
		public const int EXPIRED_PASSWORD = 63;

		/// <summary>
		/// Forbidden password (e.g. recently used)
		/// Value: 64
		/// </summary>
		public const int FORBIDDEN_PASSWORD = 64;

		/// <summary>
		/// Security credential is invalid.
		/// Value: 65
		/// </summary>
		public const int INVALID_CREDENTIAL = 65;

		/// <summary>
		/// Login session expired.
		/// Value: 66
		/// </summary>
		public const int EXPIRED_SESSION = 66;

		/// <summary>
		/// Role name is invalid.
		/// Value: 70
		/// </summary>
		public const int INVALID_ROLE = 70;

		/// <summary>
		/// Role already exists.
		/// Value: 71
		/// </summary>
		public const int ROLE_ALREADY_EXISTS = 71;
	
		/// <summary>
		/// Specified Privilege is not valid.
		/// Value: 72
		/// </summary>
		public const int INVALID_PRIVILEGE = 72;

		/// <summary>
		/// Invalid IP address whitelist.
		/// Value: 73
		/// </summary>
		public const int INVALID_WHITELIST = 73;

		/// <summary>
		/// Quotas not enabled on server.
		/// Value: 74
		/// </summary>
		public const int QUOTAS_NOT_ENABLED = 74;

		/// <summary>
		/// Invalid quota value.
		/// Value: 75
		/// </summary>
		public const int INVALID_QUOTA = 75;

		/// <summary>
		/// User must be authentication before performing database operations.
		/// Value: 80
		/// </summary>
		public const int NOT_AUTHENTICATED = 80;

		/// <summary>
		/// User does not posses the required role to perform the database operation.
		/// Value: 81
		/// </summary>
		public const int ROLE_VIOLATION = 81;

		/// <summary>
		/// Command not allowed because sender IP address not whitelisted.
		/// Value: 82
		/// </summary>
		public const int NOT_WHITELISTED = 82;

		/// <summary>
		/// Quota exceeded.
		/// Value: 83
		/// </summary>
		public const int QUOTA_EXCEEDED = 83;
		
		/// <summary>
		/// A user defined function returned an error code.
		/// Value: 100
		/// </summary>
		public const int UDF_BAD_RESPONSE = 100;

		/// <summary>
		/// Batch functionality has been disabled.
		/// Value: 150
		/// </summary>
		public const int BATCH_DISABLED = 150;

		/// <summary>
		/// Batch max requests have been exceeded.
		/// Value: 151
		/// </summary>
		public const int BATCH_MAX_REQUESTS_EXCEEDED = 151;

		/// <summary>
		/// All batch queues are full.
		/// Value: 152
		/// </summary>
		public const int BATCH_QUEUES_FULL = 152;
	
		/// <summary>
		/// Secondary index already exists.
		/// Value: 200
		/// </summary>
		public const int INDEX_ALREADY_EXISTS = 200;
		public const int INDEX_FOUND = 200; // For legacy reasons.

		/// <summary>
		/// Requested secondary index does not exist.
		/// Value: 201
		/// </summary>
		public const int INDEX_NOTFOUND = 201;

		/// <summary>
		/// Secondary index memory space exceeded.
		/// Value: 202
		/// </summary>
		public const int INDEX_OOM = 202;

		/// <summary>
		/// Secondary index not available.
		/// Value: 203
		/// </summary>
		public const int INDEX_NOTREADABLE = 203;

		/// <summary>
		/// Generic secondary index error.
		/// Value: 204
		/// </summary>
		public const int INDEX_GENERIC = 204;

		/// <summary>
		/// Index name maximum length exceeded.
		/// Value: 205
		/// </summary>
		public const int INDEX_NAME_MAXLEN = 205;

		/// <summary>
		/// Maximum number of indicies exceeded.
		/// Value: 206
		/// </summary>
		public const int INDEX_MAXCOUNT = 206;
	
		/// <summary>
		/// Secondary index query aborted.
		/// Value: 210
		/// </summary>
		public const int QUERY_ABORTED = 210;

		/// <summary>
		/// Secondary index queue full.
		/// ValueL 211
		/// </summary>
		public const int QUERY_QUEUEFULL = 211;

		/// <summary>
		/// Secondary index query timed out on server.
		/// Value: 212
		/// </summary>
		public const int QUERY_TIMEOUT = 212;

		/// <summary>
		/// Generic query error.
		/// Value: 213
		/// </summary>
		public const int QUERY_GENERIC = 213;

		/// <summary>
		/// Should connection be put back into pool.
		/// </summary>
		public static bool KeepConnection(int resultCode)
		{
			if (resultCode <= 0)
			{
				// Do not keep connection on client errors.
				return false;
			}

			switch (resultCode)
			{
				case SCAN_ABORT:
				case QUERY_ABORTED:
					return false;

				default:
					// Keep connection on TIMEOUT because it can be server response which does not 
					// close socket.  Also, client timeout code path does not call this method. 
					return true;
			}
		}
	
		/// <summary>
		/// Return result code as a string.
		/// </summary>
		public static string GetResultString(int resultCode)
		{
			switch (resultCode)
			{
			case BATCH_FAILED:
				return "One or more keys failed in a batch";

			case NO_RESPONSE:
				return "No response received from server";

			case MAX_ERROR_RATE:
				return "Max error rate exceeded";

			case MAX_RETRIES_EXCEEDED:
				return "Max retries exceeded";
				
			case SERIALIZE_ERROR:
				return "Serialize error";

			case SERVER_NOT_AVAILABLE:
				return "Server not available";

			case NO_MORE_CONNECTIONS:
				return "No more available connections";

			case COMMAND_REJECTED:
				return "Command rejected";

			case QUERY_TERMINATED:
				return "Query terminated";

			case SCAN_TERMINATED:
				return "Scan terminated";

			case INVALID_NODE_ERROR:
				return "Invalid node";

			case PARSE_ERROR:
				return "Parse error";

			case CLIENT_ERROR:
				return "Client error";

			case OK:
				return "ok";

			case SERVER_ERROR:
				return "Server error";

			case KEY_NOT_FOUND_ERROR:
				return "Key not found";

			case GENERATION_ERROR:
				return "Generation error";

			case PARAMETER_ERROR:
				return "Parameter error";

			case KEY_EXISTS_ERROR:
				return "Key already exists";

			case BIN_EXISTS_ERROR:
				return "Bin already exists";

			case CLUSTER_KEY_MISMATCH:
				return "Cluster key mismatch";

			case SERVER_MEM_ERROR:
				return "Server memory error";

			case TIMEOUT:
				return "Timeout";

			case ALWAYS_FORBIDDEN:
				return "Operation not allowed";

			case PARTITION_UNAVAILABLE:
				return "Partition unavailable";

			case BIN_TYPE_ERROR:
				return "Bin type error";

			case RECORD_TOO_BIG:
				return "Record too big";

			case KEY_BUSY:
				return "Hot key";

			case SCAN_ABORT:
				return "Scan aborted";

			case UNSUPPORTED_FEATURE:
				return "Unsupported server feature";

			case BIN_NOT_FOUND:
				return "Bin not found";

			case DEVICE_OVERLOAD:
				return "Device overload";

			case KEY_MISMATCH:
				return "Key mismatch";

			case INVALID_NAMESPACE:
				return "Namespace not found";

			case BIN_NAME_TOO_LONG:
				return "Bin name length greater than 15 characters or maximum bins exceeded";

			case FAIL_FORBIDDEN:
				return "Operation not allowed at this time";

			case ELEMENT_NOT_FOUND:
				return "Map key not found";

			case ELEMENT_EXISTS:
				return "Map key exists";

			case ENTERPRISE_ONLY:
				return "Enterprise only";

			case OP_NOT_APPLICABLE:
				return "Operation not applicable";

			case FILTERED_OUT:
				return "Transaction filtered out";

			case LOST_CONFLICT:
				return "Transaction failed due to conflict with XDR";

			case XDR_KEY_BUSY:
				return "Write can't complete until XDR finishes shipping.";

			case QUERY_END:
				return "Query end";

			case SECURITY_NOT_SUPPORTED:
				return "Security not supported";

			case SECURITY_NOT_ENABLED:
				return "Security not enabled";

			case SECURITY_SCHEME_NOT_SUPPORTED:
				return "Security scheme not supported";

			case INVALID_COMMAND:
				return "Invalid command";

			case INVALID_FIELD:
				return "Invalid field";

			case ILLEGAL_STATE:
				return "Illegal state";

			case INVALID_USER:
				return "Invalid user";

			case USER_ALREADY_EXISTS:
				return "User already exists";

			case INVALID_PASSWORD:
				return "Invalid password";

			case EXPIRED_PASSWORD:
				return "Password expired";

			case FORBIDDEN_PASSWORD:
				return "Password can't be reused";

			case INVALID_CREDENTIAL:
				return "Invalid credential";

			case EXPIRED_SESSION:
				return "Login session expired";

			case INVALID_ROLE:
				return "Invalid role";

			case ROLE_ALREADY_EXISTS:
				return "Role already exists";

			case INVALID_PRIVILEGE:
				return "Invalid privilege";

			case INVALID_WHITELIST:
				return "Invalid whitelist";

			case QUOTAS_NOT_ENABLED:
				return "Quotas not enabled";

			case INVALID_QUOTA:
				return "Invalid quota";

			case NOT_AUTHENTICATED:
				return "Not authenticated";

			case ROLE_VIOLATION:
				return "Role violation";

			case NOT_WHITELISTED:
				return "Command not whitelisted";

			case QUOTA_EXCEEDED:
				return "Quota exceeded";

			case UDF_BAD_RESPONSE:
				return "UDF returned error";

			case BATCH_DISABLED:
				return "Batch functionality has been disabled";

			case BATCH_MAX_REQUESTS_EXCEEDED:
				return "Batch max requests have been exceeded";

			case BATCH_QUEUES_FULL:
				return "All batch queues are full";

			case INDEX_ALREADY_EXISTS:
				return "Index already exists";

			case INDEX_NOTFOUND:
				return "Index not found";

			case INDEX_OOM:
				return "Index out of memory";

			case INDEX_NOTREADABLE:
				return "Index not readable";

			case INDEX_GENERIC:
				return "Index error";

			case INDEX_NAME_MAXLEN:
				return "Index name max length exceeded";

			case INDEX_MAXCOUNT:
				return "Index count exceeds max";
			
			case QUERY_ABORTED:
				return "Query aborted";

			case QUERY_QUEUEFULL:
				return "Query queue full";

			case QUERY_TIMEOUT:
				return "Query timeout";

			case QUERY_GENERIC:
				return "Query error";

			default:
				return "";
			}
		}
	}
}
