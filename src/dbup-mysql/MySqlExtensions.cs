using System;
using System.Data;
using System.Data.SqlClient;
using DbUp.Builder;
using System.Linq;
using DbUp;
using DbUp.Engine.Output;
using DbUp.MySql;
using DbUp.Engine.Transactions;
using MySql.Data.MySqlClient;

/// <summary>
/// Configuration extension methods for MySql.
/// </summary>
// ReSharper disable once CheckNamespace
public static class MySqlExtensions
{
    /// <summary>
    /// Creates an upgrader for MySql databases.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">MySql database connection string.</param>
    /// <returns>
    /// A builder for a database upgrader designed for MySql databases.
    /// </returns>
    public static UpgradeEngineBuilder MySqlDatabase(this SupportedDatabases supported, string connectionString)
    {
        foreach (var pair in connectionString.Split(';').Select(s => s.Split('=')).Where(pair => pair.Length == 2).Where(pair => pair[0].ToLower() == "database"))
        {
            return MySqlDatabase(new MySqlConnectionManager(connectionString), pair[1]);
        }

        return MySqlDatabase(new MySqlConnectionManager(connectionString));
    }

    /// <summary>
    /// Creates an upgrader for MySql databases.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">MySql database connection string.</param>
    /// <param name="schema">Which MySql schema to check for changes</param>
    /// <returns>
    /// A builder for a database upgrader designed for MySql databases.
    /// </returns>
    public static UpgradeEngineBuilder MySqlDatabase(this SupportedDatabases supported, string connectionString, string schema)
    {
        return MySqlDatabase(new MySqlConnectionManager(connectionString), schema);
    }
    
    /// <summary>
    /// Creates an upgrader for MySql databases.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionManager">The <see cref="MySqlConnectionManager"/> to be used during a database upgrade.</param>
    /// <returns>
    /// A builder for a database upgrader designed for MySql databases.
    /// </returns>
    public static UpgradeEngineBuilder MySqlDatabase(this SupportedDatabases supported, IConnectionManager connectionManager)
        => MySqlDatabase(connectionManager);

    /// <summary>
    /// Creates an upgrader for MySql databases.
    /// </summary>
    /// <param name="connectionManager">The <see cref="MySqlConnectionManager"/> to be used during a database upgrade.</param>
    /// <returns>
    /// A builder for a database upgrader designed for MySql databases.
    /// </returns>
    public static UpgradeEngineBuilder MySqlDatabase(IConnectionManager connectionManager)
    {
        return MySqlDatabase(connectionManager, null);
    }

    /// <summary>
    /// Creates an upgrader for MySql databases.
    /// </summary>
    /// <param name="connectionManager">The <see cref="MySqlConnectionManager"/> to be used during a database upgrade.</param>
    /// /// <param name="schema">Which MySQL schema to check for changes</param>
    /// <returns>
    /// A builder for a database upgrader designed for MySql databases.
    /// </returns>
    public static UpgradeEngineBuilder MySqlDatabase(IConnectionManager connectionManager, string schema)
    {
        var builder = new UpgradeEngineBuilder();
        builder.Configure(c => c.ConnectionManager = connectionManager);
        builder.Configure(c => c.ScriptExecutor = new MySqlScriptExecutor(() => c.ConnectionManager, () => c.Log, null, () => c.VariablesEnabled, c.ScriptPreprocessors, () => c.Journal));
        builder.Configure(c => c.Journal = new MySqlTableJournal(() => c.ConnectionManager, () => c.Log, schema, "schemaversions"));
        builder.WithPreprocessor(new MySqlPreprocessor());
        return builder;
    }

    /// <summary>
    /// Drop the database specified in the connection string.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns></returns>
    public static void SqlDatabase(this SupportedDatabasesForDropDatabase supported, string connectionString)
    {
        SqlDatabase(supported, connectionString, new ConsoleUpgradeLog());
    }

    /// <summary>
    /// Drop the database specified in the connection string.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="commandTimeout">Use this to set the command time out for dropping a database in case you're encountering a time out in this operation.</param>
    /// <returns></returns>
    public static void SqlDatabase(this SupportedDatabasesForDropDatabase supported, string connectionString, int commandTimeout)
    {
        SqlDatabase(supported, connectionString, new ConsoleUpgradeLog(), commandTimeout);
    }

    /// <summary>
    /// Drop the database specified in the connection string.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="logger">The <see cref="DbUp.Engine.Output.IUpgradeLog"/> used to record actions.</param>
    /// <param name="timeout">Use this to set the command time out for dropping a database in case you're encountering a time out in this operation.</param>
    /// <returns></returns>
    public static void SqlDatabase(this SupportedDatabasesForDropDatabase supported, string connectionString, IUpgradeLog logger, int timeout = -1)
    {
        GetMysqlConnectionStringBuilder(connectionString, logger, out var masterConnectionString, out var databaseName);

        using (var connection = new MySqlConnection(masterConnectionString))
        {
            connection.Open();
            //var databaseExistCommand = new MySqlCommand($"SELECT TOP 1 case WHEN dbid IS NOT NULL THEN 1 ELSE 0 end FROM sys.sysdatabases WHERE name = '{databaseName}';", connection)
            var databaseExistCommand = new MySqlCommand($"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{databaseName}';", connection)
            {
                CommandType = CommandType.Text
            };
            using (var command = databaseExistCommand)
            {
                var exists = (int?)command.ExecuteScalar();
                if (!exists.HasValue)
                    return;
            }

            //var dropDatabaseCommand = new MySqlCommand($"ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE IF EXISTS [{databaseName}];", connection) { CommandType = CommandType.Text };
            var dropDatabaseCommand = new MySqlCommand($"DROP DATABASE IF EXISTS [{databaseName}];", connection) { CommandType = CommandType.Text };
            using (var command = dropDatabaseCommand)
            {
                command.ExecuteNonQuery();
            }

            logger.WriteInformation("Dropped database {0}", databaseName);
        }
    }

    private static void GetMysqlConnectionStringBuilder(string connectionString, IUpgradeLog logger, out string masterConnectionString, out string databaseName)
    {
        if (string.IsNullOrEmpty(connectionString) || connectionString.Trim() == string.Empty)
            throw new ArgumentNullException("connectionString");

        if (logger == null)
            throw new ArgumentNullException("logger");

        var masterConnectionStringBuilder = new MySqlConnectionStringBuilder(connectionString);
        databaseName = masterConnectionStringBuilder.Database;

        if (string.IsNullOrEmpty(databaseName) || databaseName.Trim() == string.Empty)
            throw new InvalidOperationException("The connection string does not specify a database name.");

        masterConnectionStringBuilder.Database = "mysql";
        var logMasterConnectionStringBuilder = new MySqlConnectionStringBuilder(masterConnectionStringBuilder.ConnectionString)
        {
            Password = string.Empty.PadRight(masterConnectionStringBuilder.Password.Length, '*')
        };

        logger.WriteInformation("Master ConnectionString => {0}", logMasterConnectionStringBuilder.ConnectionString);
        masterConnectionString = masterConnectionStringBuilder.ConnectionString;
    }
}