/* 
 * Copyright 2012-2017 Aerospike, Inc.
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

namespace AerospikeClient.Pooled_Objects
{
    using System;
    using System.Text;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;

    internal class StringBuilderPool : ObjectPool<StringBuilder>
    {
        public StringBuilderPool(Func<StringBuilder> factory) : base(factory) { }

        public StringBuilderPool(Func<StringBuilder> factory, int size) : base(factory, size) { }

        public override void Free(StringBuilder obj)
        {
            Debug.Assert(obj != null);

            if (obj.Capacity <= 1024)
            {
                obj.Clear();
                base.Free(obj);
            }
            else
            {
                base.Forget(obj);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReturnStringAndFree(StringBuilder obj)
        {
            var result = obj.ToString();
            Free(obj);

            return result;
        }
    }
}
