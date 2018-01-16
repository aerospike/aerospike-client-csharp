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

namespace AerospikeTest.Pooled_Objects
{
    using System.Text;
    using AerospikeClient.Pooled_Objects;

    public class PooledStringBuilderTests
    {
        private readonly StringBuilderPool _sut = new StringBuilderPool(() => new StringBuilder());

        [Xunit.Fact]
        public void UsingOneInstanceAtATime_ShouldReuseTheStoredInstanceInThePool()
        {
            // Arrange
            var sb = _sut.Allocate();

            // Act
            _sut.Free(sb);

            // Assert
            Xunit.Assert.Same(sb, _sut.Allocate());
        }

        [Xunit.Fact]
        public void
            AllocatingWhenThereIsNoInstantiatedStringBuilder_ShouldCreateNewInstanceOfStringBuilder()
        {
            // Arrange
            // Act
            var sb = _sut.Allocate();
            var sb2 = _sut.Allocate();

            // Assert
            Xunit.Assert.NotSame(sb, sb2);
        }

        [Xunit.Fact]
        public void AllocatingLargeStringBuilder_ShouldAllocateInstanceButWhenItIsReleasedShouldBeForgoten()
        {
            // Arrange
            var sut = new StringBuilderPool(() => new StringBuilder(43663)); // large capacity

            // Act
            var sb = sut.Allocate();
            sut.Free(sb);
            
            // Assert
            Xunit.Assert.NotSame(sb, sut.Allocate());
        }

        [Xunit.Fact]
        public void ReturnStringAndFree_ShouldReturnTheValueOfTheBuilderAndFreeItBackToThePool()
        {
            // Arranges
            var sb = _sut.Allocate();
            sb.Append("test");

            // Act
            // Assert
            Xunit.Assert.Equal("test", _sut.ReturnStringAndFree(sb));
            Xunit.Assert.Same(sb, _sut.Allocate());
        }
    }
}
