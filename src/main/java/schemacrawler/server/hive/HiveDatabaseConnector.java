package schemacrawler.server.hive;

import java.io.IOException;
import java.util.regex.Pattern;

import schemacrawler.schemacrawler.DatabaseServerType;
import schemacrawler.tools.databaseconnector.DatabaseConnector;
import schemacrawler.tools.iosource.ClasspathInputResource;

public final class HiveDatabaseConnector extends DatabaseConnector {
	
	protected HiveDatabaseConnector() throws IOException {
		super(new DatabaseServerType("hive", "Apache Hive DataBase"),
				new ClasspathInputResource("/help/Connections.hive.txt"),
				new ClasspathInputResource("/schemacrawler-hive.config.properties"), (informationSchemaViewsBuilder,
						connection) -> informationSchemaViewsBuilder.fromResourceFolder("/hive.information_schema"),
				url -> Pattern.matches("jdbc:hive2:.*", url));
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (final ClassNotFoundException e) {
			throw new RuntimeException("Could not load Hive JDBC driver", e);
		}

	}

}
