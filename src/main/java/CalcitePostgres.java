import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.InternalProperty;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class CalcitePostgres {

	private static final SqlParser.Config SQL_PARSER_CONFIG =
			SqlParser.configBuilder(SqlParser.Config.DEFAULT)
					.setCaseSensitive(false)
					.setConformance(SqlConformanceEnum.DEFAULT)
					.setQuoting(Quoting.DOUBLE_QUOTE)
					.build();

	private static String getRandomString(Random rnd, int stringLength) {
		final int leftLimit = new Character('a');
		final int rightLimit = new Character('z');

		final StringBuilder buffer = new StringBuilder(stringLength);

		for (int i = 0; i < stringLength; i++) {
			int randomLimitedInt = leftLimit + (int)
					(rnd.nextFloat() * (rightLimit - leftLimit + 1));
			buffer.append((char) randomLimitedInt);
		}

		return buffer.toString();
	}

	private static class PlannerResult {
		private final RelNode topNode;
		private final RelDataType originalRowType;

		public PlannerResult(RelNode topNode, RelDataType originalRowType) {
			this.topNode = topNode;
			this.originalRowType = originalRowType;
		}
	}

	private static PlannerResult runPlanner(FrameworkConfig config, String query)
			throws RelConversionException, SqlParseException, ValidationException {

		final Planner planner = Frameworks.getPlanner(config);

		SqlNode n = planner.parse(query);
		n = planner.validate(n);

		final RelNode root = planner.rel(n).project();

		System.out.println("Query: " + query + " " +
				RelOptUtil.dumpPlan("-- Logical Plan", root, SqlExplainFormat.TEXT,
						SqlExplainLevel.ALL_ATTRIBUTES));

		final RelDataType originalRowType = root.getRowType();
		final RelOptCluster cluster = root.getCluster();
		final RelOptPlanner optPlanner = cluster.getPlanner();
		final RelTraitSet desiredTraits =
				cluster.traitSet().replace(EnumerableConvention.INSTANCE);
		final RelNode newRoot = optPlanner.changeTraits(root, desiredTraits);
		optPlanner.setRoot(newRoot);

		final RelNode bestExp = optPlanner.findBestExp();

		System.out.println("Query: " + query + "\n" +
				RelOptUtil.dumpPlan("-- Best  Plan", bestExp, SqlExplainFormat.TEXT,
						SqlExplainLevel.ALL_ATTRIBUTES));

		return new PlannerResult(bestExp, originalRowType);
	}

	private static void populateDB(String dbUrl, boolean dropTables) throws SQLException {
		try (final Connection con = DriverManager.getConnection(dbUrl, "postgres", "postgres")) {
			final Statement stmt1 = con.createStatement();

			if (dropTables) {
				stmt1.execute("drop table if exists table1");
				stmt1.execute("drop table if exists table2");
			}

			stmt1.execute("select count(*) from table1 limit 1");

			if (!stmt1.getResultSet().next()) {

				stmt1.execute("create table IF NOT EXISTS table1(id int not null primary key, field1 varchar, field2 int)");
				stmt1.execute("CREATE UNIQUE INDEX IF NOT EXISTS table1_id_index ON table1(id)");

				stmt1.execute("create table table2(id int not null primary key, field1 varchar, id1 int)");
				stmt1.execute("CREATE UNIQUE INDEX table2_id_index ON table2(id)");
				stmt1.execute("CREATE INDEX table2_id1_index ON table2(id1)");

				final Random rnd = new Random();

				final String sqlInsert1 = "insert into table1 values(?, ?, ?)";
				final String sqlInsert2 = "insert into table2 values(?, ?, ?)";

				final int NUM_ROWS = 100000;

				for (int i = 0; i < NUM_ROWS; ++i) {
					final PreparedStatement prepStmt1 = con.prepareStatement(sqlInsert1);
					prepStmt1.setInt(1, i);
					prepStmt1.setString(2, getRandomString(rnd, 10));
					prepStmt1.setInt(3, rnd.nextInt());
					prepStmt1.executeUpdate();

					final PreparedStatement prepStmt2 = con.prepareStatement(sqlInsert2);
					prepStmt2.setInt(1, i);
					prepStmt2.setString(2, getRandomString(rnd, 10));
					prepStmt2.setInt(3, rnd.nextInt(NUM_ROWS));
					prepStmt2.executeUpdate();
				}
			}
		}
	}

	public static void main(String... args)
			throws SQLException, ValidationException, RelConversionException, SqlParseException {

		final String postgresURL = "jdbc:postgresql://localhost:5432/calcite";
		final String sqliteURL = "jdbc:sqlite:src/main/resources/ex1";

		populateDB(postgresURL, false);
/*
		final Properties info = new Properties();
		info.setProperty("lex", "POSTGRES");
		info.setProperty("defaultNullCollation", "LAST");
		info.put(InternalProperty.CASE_SENSITIVE, true);
		info.put(InternalProperty.UNQUOTED_CASING, Casing.TO_LOWER);
		info.put(InternalProperty.QUOTED_CASING, Casing.UNCHANGED);
*/
		final Connection connection = DriverManager.getConnection("jdbc:calcite:");
		final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
		final SchemaPlus rootSchema = calciteConnection.getRootSchema();
		System.out.println(rootSchema.getName());

		final DataSource ds = JdbcSchema.dataSource(postgresURL, "org.postgresql.Driver", "postgres", "postgres");
		rootSchema.add("POSTGRES", JdbcSchema.create(rootSchema, "POSTGRES", ds, null, null));

		final DataSource ds2 = JdbcSchema.dataSource(sqliteURL, "org.sqlite.JDBC", "", "");
		rootSchema.add("SQLITE", JdbcSchema.create(rootSchema, "SQLITE", ds2, null, null));

		final String sqliteQuery = "select * from sqlite.\"tbl1\"";
		final Statement sqliteStmt = connection.createStatement();
		final ResultSet sqliteRs = sqliteStmt.executeQuery(sqliteQuery);
		while (sqliteRs.next()) {
			System.out.println(sqliteRs.getString(1) + " = " + sqliteRs.getInt(2));
		}

		System.out.println();

		plan(rootSchema, sqliteQuery);


		final String postgresQuery = "select postgres.\"table1\".\"id\", postgres.\"table1\".\"field1\", postgres.\"table2\".\"field1\" " +
				"from postgres.\"table1\" join postgres.\"table2\" " +
				"  on postgres.\"table1\".\"id\" = postgres.\"table2\".\"id1\"" +
				"where postgres.\"table1\".\"id\" < 5";

		final Statement postgresStmt = connection.createStatement();
		final ResultSet postgresRs = postgresStmt.executeQuery(postgresQuery);

		while (postgresRs.next()) {
			System.out.println(postgresRs.getString(1) + '=' + postgresRs.getString(2));
		}

		System.out.println();

		plan(rootSchema, postgresQuery);

		final String fedQuery = "select postgres.\"table1\".\"id\", postgres.\"table1\".\"field1\", postgres.\"table2\".\"field1\" " +
				"from postgres.\"table1\" join postgres.\"table2\" " +
				"  on ( postgres.\"table1\".\"id\" = postgres.\"table2\".\"id1\" ) " +
				"  join sqlite.\"tbl1\" on ( postgres.\"table1\".\"id\" = sqlite.\"tbl1\".\"two\" ) " +
				"where postgres.\"table1\".\"id\" < 5";

		final Statement fedStmt = connection.createStatement();
		final ResultSet fedRs = fedStmt.executeQuery(fedQuery);

		while (fedRs.next()) {
			System.out.println(fedRs.getString(1) + '=' + fedRs.getString(2));
		}

		System.out.println();

		plan(rootSchema, fedQuery);
	}

	private static void plan(SchemaPlus rootSchema, String query)
			throws ValidationException, RelConversionException, SqlParseException {

		final List<RelTraitDef> traitDefs = new ArrayList<>();
		traitDefs.add(ConventionTraitDef.INSTANCE);

		final FrameworkConfig config = Frameworks.newConfigBuilder()
				.parserConfig(SQL_PARSER_CONFIG)
				.defaultSchema(rootSchema)
				.traitDefs(traitDefs)
				.programs(Programs.ofRules(Programs.RULE_SET))
				.build();

		final PlannerResult plan = runPlanner(config, query);

		System.out.println(plan.originalRowType.toString());
		System.out.println(plan.topNode.toString());
	}
}
