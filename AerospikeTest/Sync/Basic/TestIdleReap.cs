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
	/// U-12: idle-trim default sits under the AWS NLB idle timeout of 350 seconds.
	/// </summary>
	[TestClass]
	public class TestIdleReap
	{
		private const double NlbIdleTimeoutSecs = 350;

		[TestMethod]
		public void DefaultTrimLimitUnderNlbIdleTimeout()
		{
			FieldInfo field = typeof(Cluster).GetField("MAX_SOCKET_IDLE_TRIM_DEFAULT_SECS",
				BindingFlags.NonPublic | BindingFlags.Static);
			Assert.IsNotNull(field);
			double trimMillis = (double)field.GetValue(null);
			double trimSecs = trimMillis / 1000.0;

			Assert.AreEqual(55.0, trimSecs, 0.001);
			Assert.IsTrue(trimSecs < NlbIdleTimeoutSecs);
			Assert.AreEqual(0, new ClientPolicy().maxSocketIdle);
		}
	}
}
