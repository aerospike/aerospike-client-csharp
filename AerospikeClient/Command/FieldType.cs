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
	public sealed class FieldType
	{
		public const int NAMESPACE = 0;
		public const int TABLE = 1;
		public const int KEY = 2;
		public const int RECORD_VERSION = 3;
		public const int DIGEST_RIPE = 4;
		public const int MRT_ID = 5;
		public const int MRT_DEADLINE = 6;
		public const int QUERY_ID = 7;
		public const int SOCKET_TIMEOUT = 9;
		public const int RECORDS_PER_SECOND = 10;
		public const int PID_ARRAY = 11;
		public const int DIGEST_ARRAY = 12;
		public const int MAX_RECORDS = 13;
		public const int BVAL_ARRAY = 15;
		public const int INDEX_RANGE = 22;
		public const int INDEX_CONTEXT = 23;
		public const int INDEX_TYPE = 26;
		public const int UDF_PACKAGE_NAME = 30;
		public const int UDF_FUNCTION = 31;
		public const int UDF_ARGLIST = 32;
		public const int UDF_OP = 33;
		public const int QUERY_BINLIST = 40;
		public const int BATCH_INDEX = 41;
		public const int FILTER_EXP = 43;
	}
}
