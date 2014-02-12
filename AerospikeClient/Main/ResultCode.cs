/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
namespace Aerospike.Client
{
	/// <summary>
	/// Database operation error codes.
	/// </summary>
	public sealed class ResultCode
	{
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
		/// Client serialization error.
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
		/// Expected cluster ID was not received.
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
		/// Database command data is invalid.
		/// </summary>
		public const int INVALID_DATA = 99;

		/// <summary>
		/// A user defined function returned an error code.
		/// </summary>
		public const int UDF_BAD_RESPONSE = 100;

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
		/// Return result code as a string.
		/// </summary>
		public static string GetResultString(int resultCode)
		{
			switch (resultCode)
			{
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

			case INVALID_DATA:
				return "Invalid command data";

			case UDF_BAD_RESPONSE:
				return "UDF returned error";

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
