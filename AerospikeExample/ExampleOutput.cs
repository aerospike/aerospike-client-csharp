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
using Aerospike.Client;
using System.Text;

namespace Aerospike.Example;

/// <summary>
/// Harness-side plumbing for example output.
/// <para>
/// Examples log with plain <see cref="System.Console"/> so their source can be lifted into
/// documentation unchanged. Everything the harness needs on top of that (timestamps, client log
/// capture, and the error count that decides pass/fail) is added here by decorating the standard
/// output streams, so no logging abstraction leaks into example code.
/// </para>
/// <para>
/// Anything an example writes to <see cref="System.Console.Error"/> counts as a failure, which is
/// how an example can fail without throwing.
/// </para>
/// </summary>
internal static class ExampleOutput
{
	private static LineWriter errorWriter;

	/// <summary>
	/// Number of error lines written since <see cref="Install"/>, including error-level messages
	/// logged by the client itself.
	/// </summary>
	public static int ErrorCount => errorWriter?.LineCount ?? 0;

	/// <summary>
	/// Decorate the console streams and route client logging through them. Disposing the result
	/// restores the original streams so harness summary output stays unadorned.
	/// </summary>
	public static IDisposable Install()
	{
		TextWriter originalOut = System.Console.Out;
		TextWriter originalError = System.Console.Error;

		errorWriter = new LineWriter(originalError);

		System.Console.SetOut(TextWriter.Synchronized(new LineWriter(originalOut)));
		System.Console.SetError(TextWriter.Synchronized(errorWriter));

		Log.SetCallback(LogCallback);

		return new Restorer(originalOut, originalError);
	}

	private static void LogCallback(Log.Level level, string message)
	{
		if (level == Log.Level.ERROR)
		{
			System.Console.Error.WriteLine($"{level} {message}");
		}
		else
		{
			System.Console.WriteLine($"{level} {message}");
		}
	}

	private sealed class Restorer(TextWriter originalOut, TextWriter originalError) : IDisposable
	{
		public void Dispose()
		{
			System.Console.SetOut(originalOut);
			System.Console.SetError(originalError);
			Log.SetCallback(null);
		}
	}

	/// <summary>
	/// Prefixes each line with a timestamp and counts the lines written. Every TextWriter overload
	/// funnels through <see cref="Write(char)"/>, so handling line boundaries here covers all of
	/// them. The underlying console writers auto-flush, so this decorator holds no state to flush.
	/// </summary>
	private sealed class LineWriter(TextWriter inner) : TextWriter
	{
		private bool atLineStart = true;
		private int lineCount;

		public int LineCount => Volatile.Read(ref lineCount);

		public override Encoding Encoding => inner.Encoding;

		public override void Write(char value)
		{
			if (atLineStart)
			{
				inner.Write($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} ");
				atLineStart = false;
			}

			inner.Write(value);

			if (value == '\n')
			{
				atLineStart = true;
				Interlocked.Increment(ref lineCount);
			}
		}
	}
}
