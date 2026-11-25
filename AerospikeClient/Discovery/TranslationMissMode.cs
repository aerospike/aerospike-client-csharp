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
	/// Action taken when an <see cref="IAddressTranslator"/> has no mapping for an advertised endpoint.
	/// See <see cref="ClientPolicy.translationOnMiss"/>.
	/// </summary>
	public enum TranslationMissMode
	{
		/// <summary>
		/// Use the advertised endpoint unchanged. YAML value: <c>pass_through</c>.
		/// </summary>
		PASS_THROUGH,

		/// <summary>
		/// Do not connect to the advertised endpoint and fail with
		/// <see cref="AerospikeException.TranslationMiss"/>. YAML value: <c>strict</c>.
		/// </summary>
		STRICT
	}
}
