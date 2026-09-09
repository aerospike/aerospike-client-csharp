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
using Aerospike.Client.Config;

namespace Aerospike.Test
{
	[TestClass]
	public class TestConfigurationDataHelpers
	{
		[TestMethod]
		public void HasMetricsWhenEnableSet()
		{
			ConfigurationDatav1_0_0 data = new();
			data.dynamicConfig.metrics.enable = true;
			Assert.IsTrue(data.HasMetrics());
		}

		[TestMethod]
		public void HasMetricsFalseWhenEnableUnset()
		{
			ConfigurationDatav1_0_0 data = new();
			data.dynamicConfig.metrics.latency_shift = 1;
			Assert.IsFalse(data.HasMetrics());
		}

		[TestMethod]
		public void HasMetricsFalseWhenConfigNull()
		{
			IConfigurationData data = null;
			Assert.IsFalse(data.HasMetrics());
		}

		[TestMethod]
		public void HasDBWCsendKeyWhenSet()
		{
			ConfigurationDatav1_0_0 data = new();
			data.dynamicConfig.batch_write.send_key = true;
			Assert.IsTrue(data.HasDBWCsendKey());
		}

		[TestMethod]
		public void HasDBUDFCsendKeyWhenSet()
		{
			ConfigurationDatav1_0_0 data = new();
			data.dynamicConfig.batch_udf.send_key = true;
			Assert.IsTrue(data.HasDBUDFCsendKey());
		}

		[TestMethod]
		public void HasDBDCsendKeyWhenSet()
		{
			ConfigurationDatav1_0_0 data = new();
			data.dynamicConfig.batch_delete.send_key = true;
			Assert.IsTrue(data.HasDBDCsendKey());
		}

		[TestMethod]
		public void HasSendKeyHelpersFalseWhenUnset()
		{
			ConfigurationDatav1_0_0 data = new();
			Assert.IsFalse(data.HasDBWCsendKey());
			Assert.IsFalse(data.HasDBUDFCsendKey());
			Assert.IsFalse(data.HasDBDCsendKey());
		}
	}
}
