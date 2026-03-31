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

namespace Aerospike.Example;

public class Console
{
	public Console()
	{
		Log.SetCallback(LogCallback);
	}

	public void Info(string format, params object[] args)
	{
		Write(Log.Level.INFO, format, args);
	}

	public void Info(string message)
	{
		Write(Log.Level.INFO, message);
	}

	public void Warn(string format, params object[] args)
	{
		Write(Log.Level.WARN, format, args);
	}

	public void Warn(string message)
	{
		Write(Log.Level.WARN, message);
	}

	public void Error(string format, params object[] args)
	{
		Write(Log.Level.ERROR, format, args);
	}

	public void Error(string message)
	{
		Write(Log.Level.ERROR, message);
	}

	public void Write(Log.Level level, string format, params object[] args)
	{
		string message = args.Length > 0 ? string.Format(format, args) : format;
		Write(level, message);
	}

	public void Write(Log.Level level, string message)
	{
		Write(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " " + level + " " + message);
	}

	public void Write(string format, params object[] args)
	{
		Write(string.Format(format, args));
	}

	public static void Write(string message)
	{
		System.Console.WriteLine(message);
	}

	private void LogCallback(Log.Level level, string message)
	{
		Write(level, message);
	}
}
