/*
 * Copyright (c) 2015, OpenSlate <mike.omalley@openslatedata.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

//package org.apache.cassandra.io.sstable

@Grab('org.apache.cassandra:cassandra-all:3.0.6')
@Grab('com.xlson.groovycsv:groovycsv:1.1')
@Grab('com.opencsv:opencsv:3.4')
@Grab('com.datastax.cassandra:cassandra-driver-core:3.0.0')


import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static com.xlson.groovycsv.CsvParser.parseCsv

import groovy.json.JsonSlurper
import com.opencsv.CSVReader
import java.text.SimpleDateFormat

import org.apache.cassandra.config.Config
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.cassandra.exceptions.InvalidRequestException
import org.apache.cassandra.io.sstable.CQLSSTableWriter

// imports for subclass of CQLSSTableWriter
import org.apache.cassandra.cql3.*
import org.apache.cassandra.cql3.functions.*
import org.apache.cassandra.cql3.Constants
import org.apache.cassandra.utils.ByteBufferUtil
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.TypeCodec
import org.apache.cassandra.cql3.statements.*
import org.apache.cassandra.schema.*
import org.apache.cassandra.exceptions.*
import org.apache.cassandra.dht.*
import org.apache.cassandra.service.*
import java.nio.ByteBuffer
import java.util.stream.Collectors


DATE_FORMAT = null
FILTERS = [:]
DECIMAL_PATTERN = ~/\.0$/
SKIP_RECORD = new Object()

class OverrideUtils {
	def static parseStatement(String query, String type)
	{
		try
		{
			ParsedStatement stmt = QueryProcessor.parseStatement(query);
	
			return UpdateStatement.ParsedInsert.class.cast(stmt);
		}
		catch (RequestValidationException e)
		{
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}
	
	static List<ColumnSpecification> prepareInsert(String insert_string)
	{
		ParsedStatement.Prepared cqlStatement = parseStatement(insert_string, "INSERT").prepare()
		UpdateStatement insert = (UpdateStatement) cqlStatement.statement;
		insert.validate(ClientState.forInternalCalls());
	
		if (insert.hasConditions())
			throw new IllegalArgumentException("Conditional statements are not supported");
		if (insert.isCounter())
			throw new IllegalArgumentException("Counter update statements are not supported");
		if (cqlStatement.boundNames.isEmpty())
			throw new IllegalArgumentException("Provided insert statement has no bind variables");
	
		return cqlStatement.boundNames;
	}
}
	
class NulllessWriterProxy extends groovy.util.Proxy {
	List<ColumnSpecification> boundNames
	List<TypeCodec> typeCodecs

	def setup(String insert)
	{
		boundNames = OverrideUtils.prepareInsert(insert)
		typeCodecs = boundNames.stream().map({bn ->  UDHelper.codecFor(UDHelper.driverType(bn.type))})
                                             .collect(Collectors.toList());
	}

	def addRow(Map<String, Object> values)
	{
		int size = boundNames.size();
		List<ByteBuffer> rawValues = new ArrayList<>(size);
		for (int i = 0; i < size; i++)
		{
			ColumnSpecification spec = boundNames.get(i);
			Object value = values.get(spec.name.toString());

			rawValues.add(value == null ? ByteBufferUtil.UNSET_BYTE_BUFFER : typeCodecs.get(i).serialize(value, ProtocolVersion.NEWEST_SUPPORTED));
		}
		return rawAddRow(rawValues);
	}
}

def load_config(filename)
{
	return new JsonSlurper().parseText(new File(filename).text)
}

def build_insert(config)
{
	def stmnt = "INSERT INTO $config.keyspace.$config.table ("
	def fieldNames = []
	def placeholders = []
	config.fields.each {
		fieldNames.add(it.name)
		placeholders.add("?")
	}
	stmnt += fieldNames.join(", ") + ") VALUES (" + placeholders.join(", ") + ")"
	return stmnt
}

def build_schema(config)
{
	def s = "CREATE TABLE $config.keyspace.$config.table ("
	def fields = []
	def clustering = config.clustering ? "WITH CLUSTERING ORDER BY ($config.clustering)" : ""
	config.fields.each {
		fields.add("$it.name $it.type")
	}
	return s + fields.join(",\n") + ",\nprimary key " + config.primary_key + ") ${clustering}"
}

def process_field(name, type, value, line, filter)
{
	if (!value || value == "NaN" || value == "Infinity") type = null

	if (!filter) {
		switch (type) {
			case null:
				value = null
				break
			case "int":
				value = value.replaceAll(DECIMAL_PATTERN, '')
				value = value.toInteger()
				break
			case "bigint":
				value = Long.parseLong(value)
				break
			case "decimal":
				value = value.replaceAll(DECIMAL_PATTERN, '')
				value = new BigDecimal(value)
				break
			case "timestamp":
				value = DATE_FORMAT.parse(value)
				break
			case "boolean":
				value = Boolean.valueOf(value)
				break
		}
	} else {
		def f
		if (FILTERS[name]) {
			f = FILTERS[name]
		} else {
			if (config.filter_imports) {
				with_imports = config.filter_imports.collect { "import " + it }.join(";") + "; " + filter
				f = evaluate(with_imports)
			} else {
				f = evaluate(filter)
			}
			f.delegate = this
			FILTERS[name] = f
		}
		value = f(value, line)
	}
	return value
}

def F(name, value, line)
{
	def f = FILTERS[name]
	return f(value, line)
}

def parse_json(string)
{
	try {
		return new JsonSlurper().parseText(string)
	} catch (e) {
		println string;
		throw e;
	}
}

def parse_csv(string)
{
	return new CSVReader(new StringReader(string));
}

def make_row(config, line)
{
	def row = [:]
	for (field in config.fields) {
		try {
			row[field.name] = process_field(field.name, field.type, line[field.name], line, config.filters.get(field.name, null))
			if (row[field.name] == SKIP_RECORD) {
				return SKIP_RECORD
			}
		} catch (Exception e) {
			println "ERROR process_field: name :: ${field.name} :: type ${field.type}"
			throw e
		}
	}
	return row
}

def main(String[] args)
{
	set_log_level()
	cli = new CliBuilder(usage: 'load.groovy -d csvfile -c configfile [-o outputdirectory] [-h] [-v]')
	cli.with {
		h longOpt: 'help', 'Show usage information'
		c longOpt: 'config', 'Config File', required: true, args:1
		d longOpt: 'data', 'Data File', required: true, args:1
		o longOpt: 'outputdir', 'Output Directory', args:1
		s longOpt: 'printschema', 'Just print the schema'
	}

	def options = cli.parse(args)
	if (!options || options.h) {
		return
	}

	config = load_config(options.c)
	DATE_FORMAT = new SimpleDateFormat(config.date_format)
	def insert_statement = build_insert(config)
	def schema = build_schema(config)

	if (options.s) {
		println schema + ';'
		return
	}

	// magic!
	Config.setClientMode(true)

	// Create output directory that has keyspace and table name in the path
	def prefix
	if (options.o) prefix = options.o
	else prefix = './data'
	File outputDir = new File(prefix + "/$config.keyspace/$config.table")
	if (!outputDir.exists() && !outputDir.mkdirs())
	{
		throw new RuntimeException("Cannot create output directory: " + outputDir)
	}

	def builder = CQLSSTableWriter.builder()
//	def builder = NoNullSSTableWriter.builder()
	builder.inDirectory(outputDir)
			.forTable(schema)
			.using(insert_statement)
			.withPartitioner(new Murmur3Partitioner())
	def writer = new NulllessWriterProxy().wrap(builder.build())
	writer.setup(insert_statement)

	String filename = options.d

	def headers = config.fields.collect { it.name }

	def data = parseCsv(
				new BufferedReader(
					new InputStreamReader(
						new FileInputStream(filename),
						"UTF-8")
					),
				readFirstLine: true,
				columnNames: headers)

	int c = 0
	try {
		for(line in data) {
			if (++c % 1000 == 0 || ! data.hasNext()) println c
			def row = make_row(config, line)
			if (row != SKIP_RECORD) {
				writer.addRow(row)
			}
		}
	} catch (Exception e) {
		println e
		println "Exception caught at data line: ${c}"
		org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e).printStackTrace()
	}
	writer.close()
}

def set_log_level() {
	Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
	root.setLevel(Level.WARN);
}

main(args)
