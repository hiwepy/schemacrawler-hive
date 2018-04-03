package schemacrawler.server.hive;

import schemacrawler.tools.databaseconnector.DatabaseConnector;
import schemacrawler.tools.databaseconnector.DatabaseServerType;

@SuppressWarnings("serial")
public final class HiveDatabaseConnector extends DatabaseConnector {

	public HiveDatabaseConnector() {
		super(new DatabaseServerType("hive", "Hive"), 
				"/help/Connections.hive.txt",
				"/schemacrawler-hive.config.properties", 
				"/hive.information_schema", "jdbc:hive2:.*");
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (final ClassNotFoundException e) {
			throw new RuntimeException("Could not load Hive JDBC driver", e);
		}
	}

}
