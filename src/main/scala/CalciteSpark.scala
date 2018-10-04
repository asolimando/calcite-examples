import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.Properties

import javax.sql.DataSource

import org.apache.calcite.avatica.InternalProperty
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.linq4j.tree.Types
import org.apache.calcite.schema.impl.{AbstractSchema, TableFunctionImpl, ViewTable}
import org.apache.calcite.schema.{SchemaPlus, TableFunction}
import org.apache.calcite.adapter.jdbc.JdbcSchema

class CalciteSpark {
  /*
    val DATABASE_URL = "jdbc:h2:mem:test"
    val SCHEMA_NAME = "PUBLIC"
    val DRIVER = "org.h2.Driver"

    val info: Properties = new Properties
    info.setProperty("lex", "MYSQL")
    info.setProperty("defaultNullCollation", "LAST")
    info.put(InternalProperty.CASE_SENSITIVE, false)
    info.put(InternalProperty.UNQUOTED_CASING, Casing.UNCHANGED)
    info.put(InternalProperty.QUOTED_CASING, Casing.UNCHANGED)

    val connection: Connection = DriverManager.getConnection("jdbc:calcite:", info)

    val calciteConnection: CalciteConnection = connection.unwrap(classOf[CalciteConnection])

    val connection: Nothing = makeDatabase
    val dataSource: DataSource = JdbcSchema.dataSource(DATABASE_URL, DRIVER, null, null)

    val rootSchema: SchemaPlus = calciteConnection.getRootSchema
    val schema: SchemaPlus = rootSchema.add("test", new AbstractSchema)
    val simple: TableFunction =
      TableFunctionImpl.create(Types.lookupMethod(classOf[ViewTest], "simple"))

    val s: SchemaPlus = rootSchema.add("si", new AbstractSchema)
    s.add("simple", simple)

    val sql = " select * from table(si.simple())"

    schema.add("emps_view",
      ViewTable.viewMacro(schema,
        sql,
        null, util.Arrays.asList("test", "emps_view"), null)
    )

    val qrySql: String = "select *from test.emps_view"
    val ps: PreparedStatement = connection.prepareStatement(qrySql)
    val resultSet: ResultSet = ps.executeQuery
  */
}