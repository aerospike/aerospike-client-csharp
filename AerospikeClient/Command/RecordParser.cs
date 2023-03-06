/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Client
{
    public sealed class RecordParser : IRecordParser
    {
        public static readonly RecordParser Instance = new RecordParser();

        public Record ParseRecord(byte[] dataBuffer, ref int dataOffsetRef, int opCount, int generation, int expiration, bool isOperation)
        {
			Dictionary<string, object> bins = new Dictionary<string, object>();
			int dataOffset = dataOffsetRef;

			for (int i = 0; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				byte particleType = dataBuffer[dataOffset + 5];
				byte nameSize = dataBuffer[dataOffset + 7];
				string name = ByteUtil.Utf8ToString(dataBuffer, dataOffset + 8, nameSize);
				dataOffset += 4 + 4 + nameSize;

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				object value = ByteUtil.BytesToParticle((ParticleType)particleType, dataBuffer, dataOffset, particleBytesSize);
				dataOffset += particleBytesSize;

				if (isOperation)
				{
					object prev;

					if (bins.TryGetValue(name, out prev))
					{
						// Multiple values returned for the same bin. 
						if (prev is OpResults)
						{
							// List already exists.  Add to it.
							OpResults list = (OpResults)prev;
							list.Add(value);
						}
						else
						{
							// Make a list to store all values.
							OpResults list = new OpResults();
							list.Add(prev);
							list.Add(value);
							bins[name] = list;
						}
					}
					else
					{
						bins[name] = value;
					}
				}
				else 
				{
					bins[name] = value;
				}
			}
			dataOffsetRef = dataOffset;
			return new Record(bins, generation, expiration);
        }

		/// <summary>
		/// Bin parsing that might be useful for custom record parsers.
		/// </summary>
		public static int ExtractBinValue(byte[] dataBuffer, int dataOffset, out string binName, out byte valueType,
	        out int valueOffset, out int valueSize)
        {
	        int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
	        valueType = dataBuffer[dataOffset + 5];
	        byte nameSize = dataBuffer[dataOffset + 7];
	        binName = ByteUtil.Utf8ToString(dataBuffer, dataOffset + 8, nameSize);
	        valueOffset = dataOffset + 4 + 4 + nameSize;
	        valueSize = opSize - (4 + nameSize);
	        return valueOffset + valueSize;
        }

        private class OpResults : List<object>
        {
	        public override string ToString()
	        {
		        return string.Join(",", base.ToArray());
	        }
        }
    }
}
