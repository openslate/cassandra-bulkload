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

@Grab('org.apache.cassandra:cassandra-all:3.0.3')
@Grab('com.xlson.groovycsv:groovycsv:1.1')
@Grab('com.opencsv:opencsv:3.4')
import static com.xlson.groovycsv.CsvParser.parseCsv

import groovy.json.JsonSlurper
import com.opencsv.CSVReader
import java.text.SimpleDateFormat

import org.apache.cassandra.config.Config
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.cassandra.exceptions.InvalidRequestException
import org.apache.cassandra.io.sstable.CQLSSTableWriter

DATE_FORMAT = null
FILTERS = [:]


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
	if (!value) type = null
	switch (type) {
		case null:
			value = null
			break
		case "int":
			value = Integer.parseInt(value)
			break
		case "bigint":
			value = Long.parseLong(value)
			break
		case "decimal":
			value = new BigDecimal(value)
			break
		case "timestamp":
			value = DATE_FORMAT.parse(value)
			break
		case "boolean":
			value = Boolean.valueOf(value)
			break
			
	}
	if (filter != null) {
		def f
		if (FILTERS[name]) {
			f = FILTERS[name]
		} else {
			if (config.filter_imports) {
				with_imports = config.filter_imports.collect { "import " + it }.join(";") + "; " + filter
				println(with_imports)
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
	config.fields.each {
		row[it.name] = process_field(it.name, it.type, line[it.name], line, config.filters.get(it.name, null))
	}
	return row
}

def main(String[] args)
{
	cli = new CliBuilder(usage: 'load.groovy -d csvfile -c configfile [-o outputdirectory] [-h]')
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
		println schema
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
	builder.inDirectory(outputDir)
			.forTable(schema)
			.using(insert_statement)
			.withPartitioner(new Murmur3Partitioner())
	def writer = builder.build()

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
	for(line in data) {
		if (++c % 1000 == 0) println c

		try {
			def row = make_row(config, line)
			writer.addRow(row)
		} catch (Exception e) {
			println "Exception caught at data line: ${c}"
			println row
			throw e
		}
	}
	writer.close()
}

main(args)
