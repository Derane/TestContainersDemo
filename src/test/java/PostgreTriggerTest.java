import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Testcontainers
public class PostgreTriggerTest {
	private Connection connection;
	private Statement statement;


	@Container
	static final GenericContainer container = new PostgreSQLContainer(
			"postgres:14.5")
			.withDatabaseName("product_db")
			.withUsername("user")
			.withPassword("password");

	static DataSource datasource;

	@BeforeEach
	void setUp() throws SQLException {
		var config = new HikariConfig();
		var jdbcContainer = (JdbcDatabaseContainer<?>) container;
		config.setJdbcUrl(jdbcContainer.getJdbcUrl());
		config.setUsername(jdbcContainer.getUsername());
		config.setPassword(jdbcContainer.getPassword());
		config.setDriverClassName(jdbcContainer.getDriverClassName());
		datasource = new HikariDataSource(config);
		connection = datasource.getConnection();
		statement = connection.createStatement();
	}

	@AfterEach
	void tearDown() throws SQLException {
		connection.close();
	}

	@org.junit.jupiter.api.Test
	void auditTriggerTest() throws Exception {
		// something that we prepraed for student
		statement.execute("CREATE TABLE STUDENTS\n" +
				"(\n" +
				"    id int primary key\n" +
				");");
		statement.execute("create table audit(\n" +
				"    id int,\n" +
				"    table_name TEXT,\n" +
				"    date timestamp\n" +
				");");
		// his solution
		String studentsQuery = """
						create function audit_function() returns trigger
				language plpgsql
				AS $$begin
				   insert into audit (id, table_name, date)
				   values(new.id,tg_table_name, now());
				   return null;
				end;
				$$;
						create trigger audit_students_trigger
				    AFTER UPDATE OR INSERT OR DELETE
				    ON STUDENTS
				    FOR EACH ROW
				    EXECUTE FUNCTION audit_function();
				""";

		statement.execute(studentsQuery);

		// pretty straightforward check but we can improve it :)
		statement.execute("""
				INSERT INTO STUDENTS VALUES (
				1);
				""");

		ResultSet resultSet = statement.executeQuery("""
				SELECT * FROM AUDIT;
				""");
		// true all of them
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt("id"));
		assertEquals("students", resultSet.getString("table_name"));
		assertEquals(LocalDate.now().getDayOfWeek(), resultSet.getDate("date").toLocalDate().getDayOfWeek());
	}

}
