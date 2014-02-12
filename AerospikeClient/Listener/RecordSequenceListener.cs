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
	/// Asynchronous result notifications for batch get and scan commands.
	/// The results are sent one record at a time.
	/// </summary>
	public interface RecordSequenceListener
	{
		/// <summary>
		/// This method is called when an asynchronous record is received from the server.
		/// The receive sequence is not ordered.
		/// <para>
		/// The user may throw a 
		/// <seealso cref="Aerospike.Client.AerospikeException.QueryTerminated"/> 
		/// exception if the command should be aborted.  If any exception is thrown, parallel command threads
		/// to other nodes will also be terminated and the exception will be propagated back through the
		/// commandFailed() call.
		/// </para>
		/// </summary>
		/// <param name="key">unique record identifier </param>
		/// <param name="record">record instance, will be null if the key is not found</param>
		/// <exception cref="AerospikeException">if error occurs or scan should be terminated.</exception>
		void OnRecord(Key key, Record record);

		/// <summary>
		/// This method is called when the asynchronous batch get or scan command completes.
		/// </summary>
		void OnSuccess();

		/// <summary>
		/// This method is called when an asynchronous batch get or scan command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}
