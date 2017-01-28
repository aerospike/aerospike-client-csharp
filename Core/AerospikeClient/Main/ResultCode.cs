/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
		/// Max connections would be exceeded.  There are no more available connections.
		/// </summary>
		public const int NO_MORE_CONNECTIONS = -7;

		/// <summary>
		/// Asynchronous max concurrent database commands have been exceeded and therefore rejected.
		/// </summary>
		public const int COMMAND_REJECTED = -6;

		/// <summary>
		/// Query was terminated by user.
		/// </summary>
		public const int QUERY_TERMINATED = -5;

		/// <summary>
		/// Scan was terminated by user.
		/// </summary>
		public const int SCAN_TERMINATED = -4;

		/// <summary>
		/// Chosen node is not currently active.
		/// </summary>
		public const int INVALID_NODE_ERROR = -3;

		/// <summary>
		/// Client parse error.
		/// </summary>
		public const int PARSE_ERROR = -2;

		/// <summary>
		/// Client serialization error.
		/// </summary>
		public const int SERIALIZE_ERROR = -1;

		/// <summary>
		/// Operation was successful.
		/// </summary>
		public const int OK = 0;

		/// <summary>
		/// Unknown server failure.
		/// </summary>
		public const int SERVER_ERROR = 1;

		/// <summary>
		/// On retrieving, touching or replacing a record that doesn't exist.
		/// </summary>
		public const int KEY_NOT_FOUND_ERROR = 2;

		/// <summary>
		/// On modifying a record with unexpected generation.
		/// </summary>
		public const int GENERATION_ERROR = 3;

		/// <summary>
		/// Bad parameter(s) were passed in database operation call.
		/// </summary>
		public const int PARAMETER_ERROR = 4;

		/// <summary>
		/// On create-only (write unique) operations on a record that already
		/// exists.
		/// </summary>
		public const int KEY_EXISTS_ERROR = 5;

		/// <summary>
		/// On create-only (write unique) operations on a bin that already
		/// exists.
		/// </summary>
		public const int BIN_EXISTS_ERROR = 6;

		/// <summary>
		/// Expected cluster was not received.
		/// </summary>
		public const int CLUSTER_KEY_MISMATCH = 7;

		/// <summary>
		/// Server has run out of memory.
		/// </summary>
		public const int SERVER_MEM_ERROR = 8;

		/// <summary>
		/// Client or server has timed out.
		/// </summary>
		public const int TIMEOUT = 9;

		/// <summary>
		/// XDS product is not available.
		/// </summary>
		public const int NO_XDS = 10;

		/// <summary>
		/// Server is not accepting requests.
		/// </summary>
		public const int SERVER_NOT_AVAILABLE = 11;

		/// <summary>
		/// Operation is not supported with configured bin type (single-bin or
		/// multi-bin).
		/// </summary>
		public const int BIN_TYPE_ERROR = 12;

		/// <summary>
		/// Record size exceeds limit.
		/// </summary>
		public const int RECORD_TOO_BIG = 13;

		/// <summary>
		/// Too many concurrent operations on the same record.
		/// </summary>
		public const int KEY_BUSY = 14;

		/// <summary>
		/// Scan aborted by server.
		/// </summary>
		public const int SCAN_ABORT = 15;

		/// <summary>
		/// Client operation not supported on connected server.
		/// </summary>
		public const int UNSUPPORTED_FEATURE = 16;

		/// <summary>
		/// Specified bin name does not exist in record.
		/// </summary>
		public const int BIN_NOT_FOUND = 17;

		/// <summary>
		/// Device not keeping up with writes.
		/// </summary>
		public const int DEVICE_OVERLOAD = 18;

		/// <summary>
		/// Key type mismatch.
		/// </summary>
		public const int KEY_MISMATCH = 19;

		/// <summary>
		/// Invalid namespace.
		/// </summary>
		public const int INVALID_NAMESPACE = 20;

		/// <summary>
		/// Bin name length greater than 14 characters or maximum bins exceeded.
		/// </summary>
		public const int BIN_NAME_TOO_LONG = 21;

		/// <summary>
		/// Operation not allowed at this time.
		/// </summary>
		public const int FAIL_FORBIDDEN = 22;

		/// <summary>
		/// Map element not found in UPDATE_ONLY write mode.
		/// </summary>
		public const int ELEMENT_NOT_FOUND = 23;

		/// <summary>
		/// Map element exists in CREATE_ONLY write mode.
		/// </summary>
		public const int ELEMENT_EXISTS = 24;
	
		/// <summary>
		/// There are no more records left for query.
		/// </summary>
		public const int QUERY_END = 50;

		/// <summary>
		/// Security functionality not supported by connected server.
		/// </summary>
		public const int SECURITY_NOT_SUPPORTED = 51;

		/// <summary>
		/// Security functionality supported, but not enabled by connected server.
		/// </summary>
		public const int SECURITY_NOT_ENABLED = 52;

		/// <summary>
		/// Security configuration not supported.
		/// </summary>
		public const int SECURITY_SCHEME_NOT_SUPPORTED = 53;
		
		/// <summary>
		/// Administration command is invalid.
		/// </summary>
		public const int INVALID_COMMAND = 54;

		/// <summary>
		/// Administration field is invalid.
		/// </summary>
		public const int INVALID_FIELD = 55;

		/// <summary>
		/// Server is in illegal stage.
		/// </summary>
		public const int ILLEGAL_STATE = 56;

		/// <summary>
		/// User name is invalid.
		/// </summary>
		public const int INVALID_USER = 60;
		
		/// <summary>
		/// User was previously created.
		/// </summary>
		public const int USER_ALREADY_EXISTS = 61;

		/// <summary>
		/// Password is invalid.
		/// </summary>
		public const int INVALID_PASSWORD = 62;

		/// <summary>
		/// Password has expired.
		/// </summary>
		public const int EXPIRED_PASSWORD = 63;

		/// <summary>
		/// Forbidden password (e.g. recently used)
		/// </summary>
		public const int FORBIDDEN_PASSWORD = 64;

		/// <summary>
		/// Security credential is invalid.
		/// </summary>
		public const int INVALID_CREDENTIAL = 65;
	
		/// <summary>
		/// Role name is invalid.
		/// </summary>
		public const int INVALID_ROLE = 70;

		/// <summary>
		/// Role already exists.
		/// </summary>
		public const int ROLE_ALREADY_EXISTS = 71;
	
		/// <summary>
		/// Specified Privilege is not valid.
		/// </summary>
		public const int INVALID_PRIVILEGE = 72;
		
		/// <summary>
		/// User must be authentication before performing database operations.
		/// </summary>
		public const int NOT_AUTHENTICATED = 80;

		/// <summary>
		/// User does not posses the required role to perform the database operation.
		/// </summary>
		public const int ROLE_VIOLATION = 81;

		/// <summary>
		/// A user defined function returned an error code.
		/// </summary>
		public const int UDF_BAD_RESPONSE = 100;

		/// <summary>
		/// The requested item in a large collection was not found.
		/// </summary>
		public const int LARGE_ITEM_NOT_FOUND = 125;

		/// <summary>
		/// Batch functionality has been disabled.
		/// </summary>
		public const int BATCH_DISABLED = 150;

		/// <summary>
		/// Batch max requests have been exceeded.
		/// </summary>
		public const int BATCH_MAX_REQUESTS_EXCEEDED = 151;

		/// <summary>
		/// All batch queues are full.
		/// </summary>
		public const int BATCH_QUEUES_FULL = 152;
	
		/// <summary>
		/// Secondary index already exists.
		/// </summary>
		public const int INDEX_FOUND = 200;

		/// <summary>
		/// Requested secondary index does not exist.
		/// </summary>
		public const int INDEX_NOTFOUND = 201;

		/// <summary>
		/// Secondary index memory space exceeded.
		/// </summary>
		public const int INDEX_OOM = 202;

		/// <summary>
		/// Secondary index not available.
		/// </summary>
		public const int INDEX_NOTREADABLE = 203;

		/// <summary>
		/// Generic secondary index error.
		/// </summary>
		public const int INDEX_GENERIC = 204;

		/// <summary>
		/// Index name maximum length exceeded.
		/// </summary>
		public const int INDEX_NAME_MAXLEN = 205;

		/// <summary>
		/// Maximum number of indicies exceeded.
		/// </summary>
		public const int INDEX_MAXCOUNT = 206;
	
		/// <summary>
		/// Secondary index query aborted.
		/// </summary>
		public const int QUERY_ABORTED = 210;

		/// <summary>
		/// Secondary index queue full.
		/// </summary>
		public const int QUERY_QUEUEFULL = 211;

		/// <summary>
		/// Secondary index query timed out on server.
		/// </summary>
		public const int QUERY_TIMEOUT = 212;

		/// <summary>
		/// Generic query error.
		/// </summary>
		public const int QUERY_GENERIC = 213;

		/// <summary>
		/// Should connection be put back into pool.
		/// </summary>
		public static bool KeepConnection(int resultCode)
		{
			// Keep connection on TIMEOUT because it can be server response which does not 
			// close socket.  Also, client timeout code path does not call this method. 
			switch (resultCode)
			{
				case 0: // Exception did not originate on server.
				case QUERY_TERMINATED:
				case SCAN_TERMINATED:
				case PARSE_ERROR:
				case SERIALIZE_ERROR:
				case SERVER_NOT_AVAILABLE:
				case SCAN_ABORT:
				case QUERY_ABORTED:
					return false;

				default:
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

			case SERIALIZE_ERROR:
				return "Serialize error";

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

			case NO_XDS:
				return "XDS not available";

			case SERVER_NOT_AVAILABLE:
				return "Server not available";

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
				return "Bin name length greater than 14 characters or maximum bins exceeded";

			case FAIL_FORBIDDEN:
				return "Operation not allowed at this time";

			case ELEMENT_NOT_FOUND:
				return "Map key not found";

			case ELEMENT_EXISTS:
				return "Map key exists";

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

			case INVALID_ROLE:
				return "Invalid role";

			case ROLE_ALREADY_EXISTS:
				return "Role already exists";

			case INVALID_PRIVILEGE:
				return "Invalid privilege";
				
			case NOT_AUTHENTICATED:
				return "Not authenticated";

			case ROLE_VIOLATION:
				return "Role violation";

			case UDF_BAD_RESPONSE:
				return "UDF returned error";

			case LARGE_ITEM_NOT_FOUND:
				return "Large collection item not found";

			case BATCH_DISABLED:
				return "Batch functionality has been disabled";

			case BATCH_MAX_REQUESTS_EXCEEDED:
				return "Batch max requests have been exceeded";

			case BATCH_QUEUES_FULL:
				return "All batch queues are full";

			case INDEX_FOUND:
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
