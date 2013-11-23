/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ExecuteCommand : ReadCommand
	{
		private readonly string packageName;
		private readonly string functionName;
		private readonly Value[] args;

		public ExecuteCommand(Cluster cluster, Policy policy, Key key, string packageName, string functionName, Value[] args) 
			: base(cluster, policy, key, null)
		{
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(key, packageName, functionName, args);
		}
	}
}