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
	/// Asynchronous result notifications for batch exists commands.
	/// The results are sent one record at a time.
	/// </summary>
	public interface ExistsSequenceListener
	{
		/// <summary>
		/// This method is called when an asynchronous batch exists result is received from the server.
		/// The receive sequence is not ordered.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		/// <param name="exists">whether key exists on server</param>
		void OnExists(Key key, bool exists);

		/// <summary>
		/// This method is called when the asynchronous batch exists command completes.
		/// </summary>
		void OnSuccess();

		/// <summary>
		/// This method is called when an asynchronous batch exists command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}
