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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Reflection helpers for white-box unit tests against internal client APIs.
	/// Prefer public APIs when possible; use these only where Java tests rely on
	/// package-private access that C# expresses as <c>internal</c>.
	/// </summary>
	internal static class Internals
	{
		internal static void SetVectorSupported(Command command, bool supported)
		{
			FieldInfo field = typeof(Command).GetField("vectorSupported", BindingFlags.Instance | BindingFlags.NonPublic);
			field.SetValue(command, supported);
		}

		internal static bool HasVector(Value value)
		{
			MethodInfo method = typeof(Value).GetMethod("HasVector", BindingFlags.Instance | BindingFlags.NonPublic);
			try
			{
				return (bool)method.Invoke(value, null);
			}
			catch (TargetInvocationException e)
			{
				throw e.InnerException ?? e;
			}
		}

		internal static bool HasVector(Expression expression)
		{
			MethodInfo method = typeof(Expression).GetMethod("HasVector", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			try
			{
				return (bool)method.Invoke(expression, null);
			}
			catch (TargetInvocationException e)
			{
				throw e.InnerException ?? e;
			}
		}

		internal static void CheckVectorSupport(Command command, bool vectorPresent)
		{
			MethodInfo method = typeof(Command).GetMethod("CheckVectorSupport", BindingFlags.Instance | BindingFlags.NonPublic);
			try
			{
				method.Invoke(command, new object[] { vectorPresent });
			}
			catch (TargetInvocationException e)
			{
				throw e.InnerException ?? e;
			}
		}

		internal static void SetReduce(Statement statement, params ReduceSpec<Record, Record>[] specs)
		{
			MethodInfo method = typeof(Statement).GetMethod("SetReduce", BindingFlags.Instance | BindingFlags.NonPublic);
			try
			{
				method.Invoke(statement, new object[] { specs });
			}
			catch (TargetInvocationException e)
			{
				throw e.InnerException ?? e;
			}
		}

		internal static ReduceSpec<Record, Record> ResolveReduce(Statement statement)
		{
			MethodInfo method = typeof(Statement).GetMethod("ResolveReduce", BindingFlags.Instance | BindingFlags.NonPublic);
			try
			{
				return (ReduceSpec<Record, Record>)method.Invoke(statement, null);
			}
			catch (TargetInvocationException e)
			{
				throw e.InnerException ?? e;
			}
		}
	}
}
