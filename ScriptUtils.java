/*
 * Copyright 2002-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.jdbc.datasource.init;

import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Generic utility methods for working with SQL scripts.
 *
 * <p>Mainly for internal use within the framework.
 *
 * @author Thomas Risberg
 * @author Sam Brannen
 * @author Juergen Hoeller
 * @author Keith Donald
 * @author Dave Syer
 * @author Chris Beams
 * @author Oliver Gierke
 * @author Chris Baldwin
 * @author Nicolas Debeissat
 * @since 4.0.3
 */
public abstract class ScriptUtils {

	/**
	 * Default statement separator within SQL scripts: {@code ";"}.
	 */
	public static final String DEFAULT_STATEMENT_SEPARATOR = ";";

	/**
	 * Fallback statement separator within SQL scripts: {@code "\n"}.
	 * <p>Used if neither a custom separator nor the
	 * {@link #DEFAULT_STATEMENT_SEPARATOR} is present in a given script.
	 */
	public static final String FALLBACK_STATEMENT_SEPARATOR = "\n";

	/**
	 * End of file (EOF) SQL statement separator: {@code "^^^ END OF SCRIPT ^^^"}.
	 * <p>This value may be supplied as the {@code separator} to {@link
	 * #executeSqlScript(Connection, EncodedResource, boolean, boolean, String, String, String, String)}
	 * to denote that an SQL script contains a single statement (potentially
	 * spanning multiple lines) with no explicit statement separator. Note that
	 * such a script should not actually contain this value; it is merely a
	 * <em>virtual</em> statement separator.
	 */
	public static final String EOF_STATEMENT_SEPARATOR = "^^^ END OF SCRIPT ^^^";

	/**
	 * Default prefix for single-line comments within SQL scripts: {@code "--"}.
	 */
	public static final String DEFAULT_COMMENT_PREFIX = "--";

	/**
	 * Default start delimiter for block comments within SQL scripts: {@code "/*"}.
	 */
	public static final String DEFAULT_BLOCK_COMMENT_START_DELIMITER = "/*";

	/**
	 * Default end delimiter for block comments within SQL scripts: <code>"*&#47;"</code>.
	 */
	public static final String DEFAULT_BLOCK_COMMENT_END_DELIMITER = "*/";


	private static final Log logger = LogFactory.getLog(ScriptUtils.class);


	/**
	 * Split an SQL script into separate statements delimited by the provided
	 * separator character. Each individual statement will be added to the
	 * provided {@code List}.
	 * <p>Within the script, {@value #DEFAULT_COMMENT_PREFIX} will be used as the
	 * comment prefix; any text beginning with the comment prefix and extending to
	 * the end of the line will be omitted from the output. Similarly,
	 * {@value #DEFAULT_BLOCK_COMMENT_START_DELIMITER} and
	 * {@value #DEFAULT_BLOCK_COMMENT_END_DELIMITER} will be used as the
	 * <em>start</em> and <em>end</em> block comment delimiters: any text enclosed
	 * in a block comment will be omitted from the output. In addition, multiple
	 * adjacent whitespace characters will be collapsed into a single space.
	 * @param script the SQL script
	 * @param separator character separating each statement &mdash; typically a ';'
	 * @param statements the list that will contain the individual statements
	 * @throws ScriptException if an error occurred while splitting the SQL script
	 * @see #splitSqlScript(String, String, List)
	 * @see #splitSqlScript(EncodedResource, String, String, String, String, String, List)
	 */
	public static void splitSqlScript(String script, char separator, List<String> statements) throws ScriptException {
		splitSqlScript(script, String.valueOf(separator), statements);
	}

	/**
	 * Split an SQL script into separate statements delimited by the provided
	 * separator string. Each individual statement will be added to the
	 * provided {@code List}.
	 * <p>Within the script, {@value #DEFAULT_COMMENT_PREFIX} will be used as the
	 * comment prefix; any text beginning with the comment prefix and extending to
	 * the end of the line will be omitted from the output. Similarly,
	 * {@value #DEFAULT_BLOCK_COMMENT_START_DELIMITER} and
	 * {@value #DEFAULT_BLOCK_COMMENT_END_DELIMITER} will be used as the
	 * <em>start</em> and <em>end</em> block comment delimiters: any text enclosed
	 * in a block comment will be omitted from the output. In addition, multiple
	 * adjacent whitespace characters will be collapsed into a single space.
	 * @param script the SQL script
	 * @param separator text separating each statement &mdash; typically a ';' or newline character
	 * @param statements the list that will contain the individual statements
	 * @throws ScriptException if an error occurred while splitting the SQL script
	 * @see #splitSqlScript(String, char, List)
	 * @see #splitSqlScript(EncodedResource, String, String, String, String, String, List)
	 */
	public static void splitSqlScript(String script, String separator, List<String> statements) throws ScriptException {
		splitSqlScript(null, script, separator, DEFAULT_COMMENT_PREFIX, DEFAULT_BLOCK_COMMENT_START_DELIMITER,
				DEFAULT_BLOCK_COMMENT_END_DELIMITER, statements);
	}

	/**
	 * Split an SQL script into separate statements delimited by the provided
	 * separator string. Each individual statement will be added to the provided
	 * {@code List}.
	 * <p>Within the script, the provided {@code commentPrefix} will be honored:
	 * any text beginning with the comment prefix and extending to the end of the
	 * line will be omitted from the output. Similarly, the provided
	 * {@code blockCommentStartDelimiter} and {@code blockCommentEndDelimiter}
	 * delimiters will be honored: any text enclosed in a block comment will be
	 * omitted from the output. In addition, multiple adjacent whitespace characters
	 * will be collapsed into a single space.
	 * @param resource the resource from which the script was read
	 * @param script the SQL script; never {@code null} or empty
	 * @param separator text separating each statement &mdash; typically a ';' or
	 * newline character; never {@code null}
	 * @param commentPrefix the prefix that identifies SQL line comments &mdash;
	 * typically "--"; never {@code null} or empty
	 * @param blockCommentStartDelimiter the <em>start</em> block comment delimiter;
	 * never {@code null} or empty
	 * @param blockCommentEndDelimiter the <em>end</em> block comment delimiter;
	 * never {@code null} or empty
	 * @param statements the list that will contain the individual statements
	 * @throws ScriptException if an error occurred while splitting the SQL script
	 */
	public static void splitSqlScript(EncodedResource resource, String script, String separator, String commentPrefix,
			String blockCommentStartDelimiter, String blockCommentEndDelimiter, List<String> statements)
			throws ScriptException {

		Assert.hasText(script, "'script' must not be null or empty");
		Assert.notNull(separator, "'separator' must not be null");
		Assert.hasText(commentPrefix, "'commentPrefix' must not be null or empty");
		Assert.hasText(blockCommentStartDelimiter, "'blockCommentStartDelimiter' must not be null or empty");
		Assert.hasText(blockCommentEndDelimiter, "'blockCommentEndDelimiter' must not be null or empty");

		StringBuilder sb = new StringBuilder();
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		boolean inEscape = false;
		for (int i = 0; i < script.length(); i++) {
			char c = script.charAt(i);
			if (inEscape) {
				inEscape = false;
				sb.append(c);
				continue;
			}
			// MySQL style escapes
			if (c == '\\') {
				inEscape = true;
				sb.append(c);
				continue;
			}
			if (!inDoubleQuote && (c == '\'')) {
				inSingleQuote = !inSingleQuote;
			}
			else if (!inSingleQuote && (c == '"')) {
				inDoubleQuote = !inDoubleQuote;
			}
			if (!inSingleQuote && !inDoubleQuote) {
				if (script.startsWith(separator, i)) {
					// We've reached the end of the current statement
					if (sb.length() > 0) {
						statements.add(sb.toString());
						sb = new StringBuilder();
					}
					i += separator.length() - 1;
					continue;
				}
				else if (script.startsWith(commentPrefix, i)) {
					// Skip over any content from the start of the comment to the EOL
					int indexOfNextNewline = script.indexOf("\n", i);
					if (indexOfNextNewline > i) {
						i = indexOfNextNewline;
						continue;
					}
					else {
						// If there's no EOL, we must be at the end of the script, so stop here.
						break;
					}
				}
				else if (script.startsWith(blockCommentStartDelimiter, i)) {
					// Skip over any block comments
					int indexOfCommentEnd = script.indexOf(blockCommentEndDelimiter, i);
					if (indexOfCommentEnd > i) {
						i = indexOfCommentEnd + blockCommentEndDelimiter.length() - 1;
						continue;
					}
					else {
						throw new ScriptParseException(
								"Missing block comment end delimiter: " + blockCommentEndDelimiter, resource);
					}
				}
				else if (c == ' ' || c == '\n' || c == '\t') {
					// Avoid multiple adjacent whitespace characters
					if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ' ') {
						c = ' ';
					}
					else {
						continue;
					}
				}
			}
			sb.append(c);
		}
		if (StringUtils.hasText(sb)) {
			statements.add(sb.toString());
		}
	}

	/**
	 * Read a script from the given resource, using "{@code --}" as the comment prefix
	 * and "{@code ;}" as the statement separator, and build a String containing the lines.
	 * @param resource the {@code EncodedResource} to be read
	 * @return {@code String} containing the script lines
	 * @throws IOException in case of I/O errors
	 */
	static String readScript(EncodedResource resource) throws IOException {
		return readScript(resource, DEFAULT_COMMENT_PREFIX, DEFAULT_STATEMENT_SEPARATOR);
	}

	/**
	 * Read a script from the provided resource, using the supplied comment prefix
	 * and statement separator, and build a {@code String} containing the lines.
	 * <p>Lines <em>beginning</em> with the comment prefix are excluded from the
	 * results; however, line comments anywhere else &mdash; for example, within
	 * a statement &mdash; will be included in the results.
	 * @param resource the {@code EncodedResource} containing the script
	 * to be processed
	 * @param commentPrefix the prefix that identifies comments in the SQL script &mdash;
	 * typically "--"
	 * @param separator the statement separator in the SQL script &mdash; typically ";"
	 * @return a {@code String} containing the script lines
	 * @throws IOException in case of I/O errors
	 */
	private static String readScript(EncodedResource resource, String commentPrefix, String separator)
			throws IOException {

		LineNumberReader lnr = new LineNumberReader(resource.getReader());
		try {
			return readScript(lnr, commentPrefix, separator);
		}
		finally {
			lnr.close();
		}
	}

	/**
	 * Read a script from the provided {@code LineNumberReader}, using the supplied
	 * comment prefix and statement separator, and build a {@code String} containing
	 * the lines.
	 * <p>Lines <em>beginning</em> with the comment prefix are excluded from the
	 * results; however, line comments anywhere else &mdash; for example, within
	 * a statement &mdash; will be included in the results.
	 * @param lineNumberReader the {@code LineNumberReader} containing the script
	 * to be processed
	 * @param commentPrefix the prefix that identifies comments in the SQL script &mdash;
	 * typically "--"
	 * @param separator the statement separator in the SQL script &mdash; typically ";"
	 * @return a {@code String} containing the script lines
	 * @throws IOException in case of I/O errors
	 */
	public static String readScript(LineNumberReader lineNumberReader, String commentPrefix, String separator)
			throws IOException {

		String currentStatement = lineNumberReader.readLine();
		StringBuilder scriptBuilder = new StringBuilder();
		while (currentStatement != null) {
			if (commentPrefix != null && !currentStatement.startsWith(commentPrefix)) {
				if (scriptBuilder.length() > 0) {
					scriptBuilder.append('\n');
				}
				scriptBuilder.append(currentStatement);
			}
			currentStatement = lineNumberReader.readLine();
		}
		appendSeparatorToScriptIfNecessary(scriptBuilder, separator);
		return scriptBuilder.toString();
	}

	private static void appendSeparatorToScriptIfNecessary(StringBuilder scriptBuilder, String separator) {
		if (separator == null) {
			return;
		}
		String trimmed = separator.trim();
		if (trimmed.length() == separator.length()) {
			return;
		}
		// separator ends in whitespace, so we might want to see if the script is trying
		// to end the same way
		if (scriptBuilder.lastIndexOf(trimmed) == scriptBuilder.length() - trimmed.length()) {
			scriptBuilder.append(separator.substring(trimmed.length()));
		}
	}

	/**
	 * Does the provided SQL script contain the specified delimiter?
	 * @param script the SQL script
	 * @param delim String delimiting each statement - typically a ';' character
	 */
	public static boolean containsSqlScriptDelimiters(String script, String delim) {
		boolean inLiteral = false;
		for (int i = 0; i < script.length(); i++) {
			if (script.charAt(i) == '\'') {
				inLiteral = !inLiteral;
			}
			if (!inLiteral && script.startsWith(delim, i)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Execute the given SQL script using default settings for statement
	 * separators, comment delimiters, and exception handling flags.
	 * <p>Statement separators and comments will be removed before executing
	 * individual statements within the supplied script.
	 * <p><strong>Warning</strong>: this method does <em>not</em> release the
	 * provided {@link Connection}.
	 * @param connection the JDBC connection to use to execute the script; already
	 * configured and ready to use
	 * @param resource the resource to load the SQL script from; encoded with the
	 * current platform's default encoding
	 * @throws ScriptException if an error occurred while executing the SQL script
	 * @see #executeSqlScript(Connection, EncodedResource, boolean, boolean, String, String, String, String)
	 * @see #DEFAULT_STATEMENT_SEPARATOR
	 * @see #DEFAULT_COMMENT_PREFIX
	 * @see #DEFAULT_BLOCK_COMMENT_START_DELIMITER
	 * @see #DEFAULT_BLOCK_COMMENT_END_DELIMITER
	 * @see org.springframework.jdbc.datasource.DataSourceUtils#getConnection
	 * @see org.springframework.jdbc.datasource.DataSourceUtils#releaseConnection
	 */
	public static void executeSqlScript(Connection connection, Resource resource) throws ScriptException {
		executeSqlScript(connection, new EncodedResource(resource));
	}

	/**
	 * Execute the given SQL script using default settings for statement
	 * separators, comment delimiters, and exception handling flags.
	 * <p>Statement separators and comments will be removed before executing
	 * individual statements within the supplied script.
	 * <p><strong>Warning</strong>: this method does <em>not</em> release the
	 * provided {@link Connection}.
	 * @param connection the JDBC connection to use to execute the script; already
	 * configured and ready to use
	 * @param resource the resource (potentially associated with a specific encoding)
	 * to load the SQL script from
	 * @throws ScriptException if an error occurred while executing the SQL script
	 * @see #executeSqlScript(Connection, EncodedResource, boolean, boolean, String, String, String, String)
	 * @see #DEFAULT_STATEMENT_SEPARATOR
	 * @see #DEFAULT_COMMENT_PREFIX
	 * @see #DEFAULT_BLOCK_COMMENT_START_DELIMITER
	 * @see #DEFAULT_BLOCK_COMMENT_END_DELIMITER
	 * @see org.springframework.jdbc.datasource.DataSourceUtils#getConnection
	 * @see org.springframework.jdbc.datasource.DataSourceUtils#releaseConnection
	 */
	public static void executeSqlScript(Connection connection, EncodedResource resource) throws ScriptException {
		executeSqlScript(connection, resource, false, false, DEFAULT_COMMENT_PREFIX, DEFAULT_STATEMENT_SEPARATOR,
				DEFAULT_BLOCK_COMMENT_START_DELIMITER, DEFAULT_BLOCK_COMMENT_END_DELIMITER);
	}

	/**
	 * Execute the given SQL script.
	 * <p>Statement separators and comments will be removed before executing
	 * individual statements within the supplied script.
	 * <p><strong>Warning</strong>: this method does <em>not</em> release the
	 * provided {@link Connection}.
	 * @param connection the JDBC connection to use to execute the script; already
	 * configured and ready to use
	 * @param resource the resource (potentially associated with a specific encoding)
	 * to load the SQL script from
	 * @param continueOnError whether or not to continue without throwing an exception
	 * in the event of an error
	 * @param ignoreFailedDrops whether or not to continue in the event of specifically
	 * an error on a {@code DROP} statement
	 * @param commentPrefix the prefix that identifies single-line comments in the
	 * SQL script &mdash; typically "--"
	 * @param separator the script statement separator; defaults to
	 * {@value #DEFAULT_STATEMENT_SEPARATOR} if not specified and falls back to
	 * {@value #FALLBACK_STATEMENT_SEPARATOR} as a last resort; may be set to
	 * {@value #EOF_STATEMENT_SEPARATOR} to signal that the script contains a
	 * single statement without a separator
	 * @param blockCommentStartDelimiter the <em>start</em> block comment delimiter; never
	 * {@code null} or empty
	 * @param blockCommentEndDelimiter the <em>end</em> block comment delimiter; never
	 * {@code null} or empty
	 * @throws ScriptException if an error occurred while executing the SQL script
	 * @see #DEFAULT_STATEMENT_SEPARATOR
	 * @see #FALLBACK_STATEMENT_SEPARATOR
	 * @see #EOF_STATEMENT_SEPARATOR
	 * @see org.springframework.jdbc.datasource.DataSourceUtils#getConnection
	 * @see org.springframework.jdbc.datasource.DataSourceUtils#releaseConnection
	 */
	public static void executeSqlScript(Connection connection, EncodedResource resource, boolean continueOnError,
			boolean ignoreFailedDrops, String commentPrefix, String separator, String blockCommentStartDelimiter,
			String blockCommentEndDelimiter) throws ScriptException {

		try {
			if (logger.isInfoEnabled()) {
				logger.info("Executing SQL script from " + resource);
			}
			long startTime = System.currentTimeMillis();

			String script;
			try {
				script = readScript(resource, commentPrefix, separator);
			}
			catch (IOException ex) {
				throw new CannotReadScriptException(resource, ex);
			}

			if (separator == null) {
				separator = DEFAULT_STATEMENT_SEPARATOR;
			}
			if (!EOF_STATEMENT_SEPARATOR.equals(separator) && !containsSqlScriptDelimiters(script, separator)) {
				separator = FALLBACK_STATEMENT_SEPARATOR;
			}

			List<String> statements = new LinkedList<String>();
			splitSqlScript(resource, script, separator, commentPrefix, blockCommentStartDelimiter,
					blockCommentEndDelimiter, statements);

			int stmtNumber = 0;
			Statement stmt = connection.createStatement();
			try {
				//某些创建脚本被工具分割开了。这类脚本需要整体执行
				StringBuilder bigSql = new StringBuilder();
				String statement1 ="";
				for (String statement : statements) {
					//暂未找到工具支持以下语法。转换成普通sql执行
					if(statement.contains("prompt")){
						Integer length = statement.split("prompt").length ;
						if(length > 3 ){
							statement= statement.substring(statement.lastIndexOf("prompt")+"prompt".length());
						}
					}
					if(statement.contains("spool off")){
						statement = statement.replace("spool off"," ");
					}
					
					
					if(statement.endsWith("/")){
						statement =	statement.substring(0, statement.lastIndexOf("/"));
					}
					
					if(!"".equals(statement.trim()) && !statement.equals("commit")){
//						bigSql.append(" ")
//							.append(statement)
//							.append("; ")
//							.append("\r\n ");
						statement1 += " " +statement +"; ";
						
					}
					
				}
				
//				String statement1 =bigSql.toString();
				if((statement1.trim().startsWith("create ")||statement1.trim().startsWith("CREATE "))  
						&&(!statement1 .contains("table ") && !statement1 .contains("TABLE "))){
					if(statement1.trim().endsWith(";")){
						statement1 = statement1.trim().substring(0, statement1.trim().lastIndexOf(";"));
					} 
					
					executeSqlScript(connection, statement1, resource);
					return;
				}
				
				for (String statement : statements) {
					if(statement.contains("prompt")){
						Integer length = statement.split("prompt").length ;
						if(length > 3 ){
							statement= statement.substring(statement.lastIndexOf("prompt")+"prompt".length());
						}
					}
					if(statement.contains("spool off")){
						statement = statement.replace("spool off"," ");
					}
					
					stmtNumber++;
					try {
						//如果不包含ddl關鍵字，不執行sql
						if(statement.startsWith("/ ")){
							statement =statement.replace("/ ", "");
						}
						
						String trim = statement.toLowerCase().trim();
						boolean cloudExe = false;
						if(trim.startsWith("alter ")||trim.startsWith("comment ")||
								trim.startsWith("insert ")||trim.startsWith("update ")||
								trim.startsWith("delete ")||trim.startsWith("commit ")||
								trim.startsWith("create ")
								){
							cloudExe = true;
						}
						if(cloudExe){
							stmt.execute(statement);
							int rowsAffected = stmt.getUpdateCount();
							if (logger.isDebugEnabled()) {
								logger.debug(rowsAffected + " returned as update count for SQL: " + statement);
								SQLWarning warningToLog = stmt.getWarnings();
								while (warningToLog != null) {
									logger.debug("SQLWarning ignored: SQL state '" + warningToLog.getSQLState() +
											"', error code '" + warningToLog.getErrorCode() +
											"', message [" + warningToLog.getMessage() + "]");
									warningToLog = warningToLog.getNextWarning();
								}
							}
						}else{
							logger.debug("第一次拆分語句不包含可執行sql:"+statement);
						}
						
					}
					catch (SQLException ex) {
						boolean dropStatement = StringUtils.startsWithIgnoreCase(statement.trim(), "drop");
						if (continueOnError || (dropStatement && ignoreFailedDrops)) {
							if (logger.isDebugEnabled()) {
								logger.debug(ScriptStatementFailedException.buildErrorMessage(statement, stmtNumber, resource), ex);
							}
						}
						else {
							logger.error(ScriptStatementFailedException.buildErrorMessage(statement, stmtNumber, resource), ex);
//							throw new ScriptStatementFailedException(statement, stmtNumber, resource, ex);
						}
					}
				}
				
				//执行混合sql文件。例如base.sql 。重新执行非创建table的创建语法。
				//大多数执行的是工具生成的sql，暂不处理手写不标准的sql
				String newScript = script.replaceAll("create ", "CREATE ");
				String[] split = newScript.split("\nCREATE ");
				if(split.length >0){
					for (String createSql : split) {
						
						String[] pices = createSql.split("\n");
						StringBuilder statementSql = new StringBuilder();
						boolean sqlFlag = true;
						String firstCreateSql = "";
						if(pices.length>1){
							for (String sqlPice : pices) {
								String trim = sqlPice.trim().toLowerCase();
								//特殊处理的语句不需要包含以下开头的语句
//								if(sqlPice.contains("alter ")||sqlPice.contains("comment ")||
//										sqlPice.contains("insert ")||sqlPice.contains("update ")||
//										sqlPice.contains("delete ")||sqlPice.contains("commit ")
//										){
//									break;
//								}
								//有些create 跟了crud等sql ，但是不需要这样的sql。就在这里作一次记录
								if(trim.startsWith("insert ")||trim.startsWith("update ")||
										trim.startsWith("delete ")
										){
									if(sqlFlag){
										firstCreateSql  = statementSql.toString();
									}
									sqlFlag = false;
								}
								//遇见非crud sql 执行断掉循环。
								if(trim.startsWith("alter ")||trim.startsWith("comment ")||
										trim.startsWith("commit ")||trim.startsWith("drop ") ){
									if(sqlFlag){
										firstCreateSql  = statementSql.toString();
									}
									break;
								}
								
								if(sqlPice.startsWith("prompt")||sqlPice.startsWith("PROMPT")
										||sqlPice .equals("/")){
									
								}else{
									statementSql.append(sqlPice)
										.append(" \n");
								}
							}
						}
						createSql =statementSql.toString();
						
						if(createSql.contains("spool off")){
							createSql = createSql.replace("spool off"," ");
						}
						
						if(createSql.endsWith("/")){
							createSql =	createSql.substring(0, createSql.lastIndexOf("/"));
						}
						
						//如果是 测试语句类型
						
						if(createSql.contains("begin")||createSql.contains("BEGIN")){
							createSql =createSql.replaceAll("end", "END");
							
							createSql=	createSql.substring(0, createSql.lastIndexOf("END")) +" END;";
						}
						
						if(firstCreateSql.contains("spool off")){
							firstCreateSql = firstCreateSql.replace("spool off"," ");
						}
						
						if(firstCreateSql.endsWith("/")){
							firstCreateSql =	firstCreateSql.substring(0, firstCreateSql.lastIndexOf("/"));
						}
						
						if(createSql.trim()!="" && createSql.length()>5){
							createSql = "CREATE " +createSql;
							firstCreateSql = "CREATE " +firstCreateSql;
//							executeSqlScript(connection, createSql, resource);
							
							try {
								stmt.execute(createSql);
							} catch (Exception e) {
								logger.debug("尝试执行："+createSql+"   失败！");
								//创建表单的语句不执行，前面已经执行过了
								if(!firstCreateSql.contains("table ")&&!firstCreateSql.contains("TABLE ")
										&&firstCreateSql.length() >10 ){
									executeSqlScript(connection, firstCreateSql, resource);
								}
							}
							
						}
					}
				}
				
				
			}
			finally {
				try {
					stmt.close();
				}
				catch (Throwable ex) {
					logger.debug("Could not close JDBC Statement", ex);
				}
			}

			long elapsedTime = System.currentTimeMillis() - startTime;
			if (logger.isInfoEnabled()) {
				logger.info("Executed SQL script from " + resource + " in " + elapsedTime + " ms.");
			}
		}
		catch (Exception ex) {
			if (ex instanceof ScriptException) {
				throw (ScriptException) ex;
			}
			throw new UncategorizedScriptException(
				"Failed to execute database script from resource [" + resource + "]", ex);
		}
	}
	
	public static void executeSqlScript(Connection connection,String statement, EncodedResource resource) throws ScriptException{
		try {
			Statement stmt = connection.createStatement();
			stmt.execute(statement);
		} catch (SQLException ex) {
			if(ex.getMessage().contains("无效字符")){
				String statementByTrim = statement.trim();
				if(statementByTrim.endsWith(";")){
					statement = statementByTrim.substring(0, statementByTrim.length() -1);
				}
				try {
					Statement stmt2 = connection.createStatement();
					stmt2.execute(statement);
				} catch (SQLException e) {
					logger.error(ScriptStatementFailedException.buildErrorMessage(statement, 0, resource), e);
				}
			}else{
				boolean dropStatement = StringUtils.startsWithIgnoreCase(statement.trim(), "drop");
				if ( (dropStatement )) {
					if (logger.isDebugEnabled()) {
						logger.debug(ScriptStatementFailedException.buildErrorMessage(statement, 0, resource), ex);
					}
				}
				else {
					logger.error(ScriptStatementFailedException.buildErrorMessage(statement, 0, resource), ex);
				}
			}
			
			
		}
		
	}
	
}
