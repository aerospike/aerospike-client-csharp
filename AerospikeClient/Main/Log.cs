/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Aerospike client logging facility. Logs can be filtered and message callbacks 
	/// can be defined to control how log messages are written.
	/// </summary>
	public sealed class Log
	{
		/// <summary>
		/// Log escalation level.
		/// </summary>
		public enum Level
		{
			/// <summary>
			/// Error condition has occurred.
			/// </summary>
			ERROR,

			/// <summary>
			/// Unusual non-error condition has occurred.
			/// </summary>
			WARN,

			/// <summary>
			/// Normal information message.
			/// </summary>
			INFO,

			/// <summary>
			/// Message used for debugging purposes.
			/// </summary>
			DEBUG
		}

		/// <summary>
		/// Additional context sent to log callback messages.
		/// </summary>
		public sealed class Context
		{
			/// <summary>
			/// Empty context for use when context is not available.
			/// </summary>
			public static readonly Context Empty = new Context("");

			/// <summary>
			/// Cluster name. Will be empty string if <see cref="ClientPolicy.clusterName"/> is not
			/// defined or the log message is not associated with a cluster.
			/// </summary>
			public readonly string clusterName;

			internal Context(string clusterName)
			{
				this.clusterName = clusterName;
			}
		}

		/// <summary>
		/// Log callback message definition. 
		/// </summary>
		/// <param name="level">message severity level</param>
		/// <param name="message">message string</param>
		public delegate void Callback(Level level, string message);

		/// <summary>
		/// Log callback message definition with additional context. 
		/// </summary>
		/// <param name="context">message context</param>
		/// <param name="level">message severity level</param>
		/// <param name="message">message string</param>
		public delegate void ContextCallback(Context context, Level level, string message);

		private static Level LogLevel = Level.INFO;
		private static Callback LogCallback = null;
		private static ContextCallback LogContextCallback = null;
		private static bool LogEnabled = false;
		private static bool LogSet = false;

		/// <summary>
		/// Set log level filter.
		/// </summary>
		/// <param name="level">only show logs at this or more urgent level</param>
		public static void SetLevel(Level level)
		{
			LogLevel = level;
		}

		/// <summary>
		/// Set log callback. To silence the log, set callback to null.
		/// This method is mutually exclusive with <see cref="SetContextCallback(ContextCallback)"/>.
		/// </summary>
		/// <param name="callback">Log callback message definition</param>
		public static void SetCallback(Callback callback)
		{
			if (callback != null)
			{
				LogCallback = callback;
				LogEnabled = true;
				LogSet = true;
			}
			else
			{
				Disable();
			}
		}

		/// <summary>
		/// Set log callback with additional context. To silence the log, set callback to null.
		/// This method is mutually exclusive with <see cref="SetCallback(Callback)"/>.
		/// </summary>
		/// <param name="callback">Log callback message definition</param>
		public static void SetContextCallback(ContextCallback callback)
		{
			if (callback != null)
			{
				LogContextCallback = callback;
				LogEnabled = true;
				LogSet = true;
			}
			else
			{
				Disable();
			}
		}

		/// <summary>
		/// Log messages to terminal standard output with timestamp, level and message.
		/// </summary>
		public static void SetCallbackStandard()
		{
			new Log.Standard();
		}

		/// <summary>
		/// Silence the log.
		/// </summary>
		public static void Disable()
		{
			LogCallback = null;
			LogContextCallback = null;
			LogEnabled = false;
			LogSet = true;
		}

		/// <summary>
		/// Determine if log callback has been set by the user.
		/// </summary>
		public static bool IsSet()
		{
			return LogSet;
		}

		/// <summary>
		/// Determine if warning log level is enabled.
		/// </summary>
		public static bool WarnEnabled()
		{
			return LogEnabled && Level.WARN <= LogLevel;
		}

		/// <summary>
		/// Determine if info log level is enabled.
		/// </summary>
		public static bool InfoEnabled()
		{
			return LogEnabled && Level.INFO <= LogLevel;
		}

		/// <summary>
		/// Determine if debug log level is enabled.
		/// </summary>
		public static bool DebugEnabled()
		{
			return LogEnabled && Level.DEBUG <= LogLevel;
		}

		/// <summary>
		/// Log an error message.
		/// </summary>
		public static void Error(string message)
		{
			LogMessage(Context.Empty, Level.ERROR, message);
		}

		/// <summary>
		/// Log an error message with additional context.
		/// </summary>
		public static void Error(Context context, string message)
		{
			LogMessage(context, Level.ERROR, message);
		}

		/// <summary>
		/// Log a warning message.
		/// </summary>
		public static void Warn(string message)
		{
			LogMessage(Context.Empty, Level.WARN, message);
		}

		/// <summary>
		/// Log a warning message with additional context.
		/// </summary>
		public static void Warn(Context context, string message)
		{
			LogMessage(context, Level.WARN, message);
		}

		/// <summary>
		/// Log an info message.
		/// </summary>
		public static void Info(string message)
		{
			LogMessage(Context.Empty, Level.INFO, message);
		}

		/// <summary>
		/// Log an info message with additional context.
		/// </summary>
		public static void Info(Context context, string message)
		{
			LogMessage(context, Level.INFO, message);
		}

		/// <summary>
		/// Log a debug message.
		/// </summary>
		public static void Debug(string message)
		{
			LogMessage(Context.Empty, Level.DEBUG, message);
		}

		/// <summary>
		/// Log a debug message with additional context. 
		/// </summary>
		public static void Debug(Context context, string message)
		{
			LogMessage(context, Level.DEBUG, message);
		}

		/// <summary>
		/// Filter and forward message to callback.
		/// </summary>
		/// <param name="level">message severity level</param>
		/// <param name="message">message string</param>
		public static void LogMessage(Level level, string message)
		{
			LogMessage(Context.Empty, level, message);
		}

		/// <summary>
		/// Filter and forward message with additional context to callback. 
		/// </summary>
		/// <param name="context">message context</param>
		/// <param name="level">message severity level</param>
		/// <param name="message">message string</param>
		public static void LogMessage(Context context, Level level, string message)
		{
			if (level <= LogLevel)
			{
				// LogContextCallback takes precedence over LogCallback.
				if (LogContextCallback != null)
				{
					LogContextCallback(context, level, message);
				}
				else
				{
					LogCallback?.Invoke(level, message);
				}
			}
		}

		private class Standard
		{
			public Standard()
			{
				Log.SetContextCallback(LogCallback);
			}

			public void LogCallback(Context context, Level level, string message)
			{
				StringBuilder sb = new StringBuilder(message.Length + 128);
				sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

				if (context.clusterName.Length > 0)
				{
					sb.Append(' ');
					sb.Append(context.clusterName);
				}

				sb.Append(' ');
				sb.Append(level.ToString());
				sb.Append(' ');
				sb.Append(message);
				Console.WriteLine(sb.ToString());
			}
		}
	}
}
