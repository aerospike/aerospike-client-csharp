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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Client-side join definition. The left record must contain a bin with a 
	/// list of keys.  That list will be used to retreive other records in the
	/// given namespace and set.
	/// </summary>
	public sealed class Join
	{
		public string leftKeysBinName;
		public string rightNamespace;
		public string rightSetName;
		public string[] rightBinNames;

		/// <summary>
		/// Create join definition to read all joined bins.
		/// </summary>
		/// <param name="leftKeysBinName">bin name of key list located in left record</param>
		/// <param name="rightNamespace">namespace of joined record(s)</param>
		/// <param name="rightSetName">set name of joined record(s)</param>
		public Join(string leftKeysBinName, string rightNamespace, string rightSetName)
		{
			this.leftKeysBinName = leftKeysBinName;
			this.rightNamespace = rightNamespace;
			this.rightSetName = rightSetName;
		}

		/// <summary>
		/// Create join definition to read specified joined bins.
		/// </summary>
		/// <param name="leftKeysBinName">bin name of key list located in main record</param>
		/// <param name="rightNamespace">namespace of joined record(s)</param>
		/// <param name="rightSetName">set name of joined record(s)</param>
		/// <param name="rightBinNames">bin names to retrieved in joined record(s)</param>
		public Join(string leftKeysBinName, string rightNamespace, string rightSetName, params string[] rightBinNames)
		{
			this.leftKeysBinName = leftKeysBinName;
			this.rightNamespace = rightNamespace;
			this.rightSetName = rightSetName;
			this.rightBinNames = rightBinNames;
		}
	}
}
