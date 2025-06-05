/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

namespace Aerospike.Client.Config
{
	public static class ConfigurationDataHelpers
	{
		public static bool HasDBWCsendKey(this ConfigurationData configData)
		{
			return configData.dynamicConfig != null && 
				configData.dynamicConfig.batch_write != null && 
				configData.dynamicConfig.batch_write.send_key != null;
		}

		public static bool HasDBUDFCsendKey(this ConfigurationData configData)
		{
			return configData.dynamicConfig != null &&
				configData.dynamicConfig.batch_udf != null &&
				configData.dynamicConfig.batch_udf.send_key != null;
		}

		public static bool HasDBDCsendKey(this ConfigurationData configData)
		{
			return configData.dynamicConfig != null &&
				configData.dynamicConfig.batch_delete != null &&
				configData.dynamicConfig.batch_delete.send_key != null;
		}
	}
}
