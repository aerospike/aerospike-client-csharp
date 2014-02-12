/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
namespace Aerospike.Client
{
	/// <summary>
	/// Container object for policy attributes used in write operations.
	/// This object is passed into methods where database writes can occur.
	/// </summary>
	public sealed class WritePolicy : Policy
	{
		/// <summary>
		/// Qualify how to handle writes where the record already exists.
		/// </summary>
		public RecordExistsAction recordExistsAction = RecordExistsAction.UPDATE;

		/// <summary>
		/// Qualify how to handle record writes based on record generation. The default (NONE)
		/// indicates that the generation is not used to restrict writes.
		/// </summary>
		public GenerationPolicy generationPolicy = GenerationPolicy.NONE;
	
		/// <summary>
		/// Expected generation. Generation is the number of times a record has been modified
		/// (including creation) on the server. If a write operation is creating a record, 
		/// the expected generation would be 0.  
		/// </summary>
		public int generation;

		/// <summary>
		/// Record expiration.  Also known as ttl (time to live). 
        /// Seconds record will live before being removed by the server.
        /// <para>
        /// Expiration values:
        /// <list type="bullet">
        /// <item>-1: Never expire for Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server
        /// versions >= 3.1.4.  For older servers, -1 means a very long (max integer) expiration.</item>
        /// <item>0:  Default to namespace's "default-ttl" on the server.</item>
        /// <item>> 0: Actual expiration in seconds.</item>
        /// </list>
        /// </para>
		/// </summary>
		public int expiration;
	}
}
