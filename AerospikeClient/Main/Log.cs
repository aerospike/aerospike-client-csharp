/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
		/// The log is written to the Windows Event Log by default.
		/// The caller can override this and set a private method which will be called 
		/// for each log entry.
		/// </summary>
		/// <param name="level">Log level.</param>
		/// <param name="msg">Log message.</param>
		public delegate void Callback(Level level, string msg);

		private static Level LogLevel = Level.INFO;
		private static Callback LogCallback = null;

		/// <summary>
		/// Set log level filter.
		/// </summary>
		/// <param name="level">only show logs at this or more urgent level</param>
		public static void SetLevel(Level level)
		{
			LogLevel = level;
		}

		/// <summary>
		/// Set optional log callback implementation. If the callback is not defined (or null), 
		/// log messages will not be displayed.
		/// </summary>
		/// <param name="callback"><seealso cref="Callback"/> implementation</param>
		public static void SetCallback(Callback callback)
		{
			LogCallback = callback;
		}

		/// <summary>
		/// Determine if warning log level is enabled.
		/// </summary>
		public static bool WarnEnabled()
		{
			return Level.WARN <= LogLevel;
		}

		/// <summary>
		/// Determine if info log level is enabled.
		/// </summary>
		public static bool InfoEnabled()
		{
			return Level.INFO <= LogLevel;
		}

		/// <summary>
		/// Determine if debug log level is enabled.
		/// </summary>
		public static bool DebugEnabled()
		{
			return Level.DEBUG <= LogLevel;
		}

		/// <summary>
		/// Log an error message. 
		/// </summary>
		/// <param name="message">message string not terminated with a newline</param>
		public static void Error(string message)
		{
			LogMessage(Level.ERROR, message);
		}

		/// <summary>
		/// Log a warning message. 
		/// </summary>
		/// <param name="message">message string not terminated with a newline</param>
		public static void Warn(string message)
		{
			LogMessage(Level.WARN, message);
		}

		/// <summary>
		/// Log an info message. 
		/// </summary>
		/// <param name="message">message string not terminated with a newline</param>
		public static void Info(string message)
		{
			LogMessage(Level.INFO, message);
		}

		/// <summary>
		/// Log an debug message. 
		/// </summary>
		/// <param name="message">message string not terminated with a newline</param>
		public static void Debug(string message)
		{
			LogMessage(Level.DEBUG, message);
		}

		/// <summary>
		/// Filter and forward message to callback.
		/// </summary>
		/// <param name="level">message severity level</param>
		/// <param name="message">message string not terminated with a newline</param>
		public static void LogMessage(Level level, string message)
		{
			if (LogCallback != null && level <= LogLevel)
			{
				LogCallback(level, message);
			}
		}
	}
}