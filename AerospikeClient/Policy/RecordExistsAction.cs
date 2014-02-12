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
	/// How to handle writes when the record already exists.
	/// </summary>
	public enum RecordExistsAction
	{
		/// <summary>
		/// Create or update record.
		/// Merge write command bins with existing bins.
		/// </summary>
		UPDATE,

		/// <summary>
		/// Update record only. Fail if record does not exist.
		/// Merge write command bins with existing bins.
		/// </summary>
		UPDATE_ONLY,

		/// <summary>
		/// Create or update record.
		/// Delete existing bins not referenced by write command bins.
		/// Supported by Aerospike 2 server versions >= 2.7.5 and 
		/// Aerospike 3 server versions >= 3.1.6.
		/// </summary>
		REPLACE,

		/// <summary>
		/// Update record only. Fail if record does not exist.
		/// Delete existing bins not referenced by write command bins.
		/// Supported by Aerospike 2 server versions >= 2.7.5 and 
		/// Aerospike 3 server versions >= 3.1.6.
		/// </summary>
		REPLACE_ONLY,

		/// <summary>
		/// Create only.  Fail if record exists. 
		/// </summary>
		CREATE_ONLY,

		[System.Obsolete("Use GenerationPolicy.EXPECT_GEN_EQUAL in WritePolicy.generationPolicy instead.")]
		EXPECT_GEN_EQUAL,

		[System.Obsolete("Use GenerationPolicy.EXPECT_GEN_GT in WritePolicy.generationPolicy instead.")]
		EXPECT_GEN_GT,

		[System.Obsolete("Use RecordExistsAction.CREATE_ONLY instead.")]
		FAIL
	}
}
