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
	/// String operation policy.
	/// <para>
	/// This is a per-operation policy carrying <see cref="StringWriteFlags"/>. It is
	/// passed inline to each <see cref="StringOperation"/> builder method and is
	/// <b>not</b> part of the client's dynamic configuration: there is no
	/// <see cref="StringPolicy.Default"/> on <see cref="ClientPolicy"/>
	/// and no corresponding stanza in the YAML config schema. Changing the flags
	/// requires constructing a new <see cref="StringPolicy"/> and passing it to the
	/// operation, not editing a config file at runtime. This mirrors how
	/// <see cref="BitPolicy"/> and <see cref="HLLPolicy"/> are scoped.
	/// </para>
	/// </summary>
	/// <remarks>
	/// Use specified <see cref="StringWriteFlags"/> when performing <see cref="StringOperation"/> modify operations.
	/// </remarks>
	public sealed class StringPolicy(StringWriteFlags flags)
	{
		/// <summary>
		/// Default string bin write semantics.
		/// </summary>
		public static readonly StringPolicy Default = new();

		internal readonly StringWriteFlags flags = flags;

		/// <summary>
		/// Use default <see cref="StringWriteFlags"/> when performing <see cref="StringOperation"/> modify operations.
		/// </summary>
		public StringPolicy() : this(StringWriteFlags.DEFAULT) { }
	}
}
