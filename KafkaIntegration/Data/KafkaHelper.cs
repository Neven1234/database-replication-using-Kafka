using Confluent.Kafka;
using DevExpress.Xpo.DB.Helpers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace KafkaIntegration.Data
{
    public class KafkaHelper
    {
        private string connString;
        private string mirrorConString;
        private ColumnChange columnChange;
        public KafkaHelper()
        {
            connString = ConfigurationManager.ConnectionStrings["primerConStr"].ConnectionString;
            mirrorConString= ConfigurationManager.ConnectionStrings["replicationServ"].ConnectionString;
        }

        // get the last
        private long GetLastSyncVersion()
        {
            string connectionString = connString; // Replace with your connection string
            long lastSyncVersion = 0;

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string query = "SELECT LastSyncVersion FROM SyncTracking ORDER BY Id DESC;"; // Get the most recent version
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();

                lastSyncVersion = (long)cmd.ExecuteScalar(); // Get the last sync version
            }

            return lastSyncVersion;
        }

        // to update the syncTraking table
        private void UpdateLastSyncVersion(long newSyncVersion)
        {
            string connectionString = connString; // Replace with your connection string

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string query = "UPDATE SyncTracking SET LastSyncVersion = @NewSyncVersion;";
                SqlCommand cmd = new SqlCommand(query, conn);
                cmd.Parameters.AddWithValue("@NewSyncVersion", newSyncVersion);
                conn.Open();

                cmd.ExecuteNonQuery();
            }
        }

        //check for updates
        //check for updates

        public async Task CheckForChanges(List<string> columnNames)
        {
           
            columnChange = new ColumnChange();
            List<string> changedColumnsNames = new List<string>();
            long lastSyncVersion = GetLastSyncVersion();  // Get the last sync version
            string connectionString = connString; // Replace with your connection string
            string query = $"SELECT * FROM CHANGETABLE(CHANGES dbo.Employees, 70) AS CT";

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();

                var changes = new List<(string changeType, string employeeId, long sysChangeVersion, byte[] changedColumn)>(); // List to store changes

                // First read the changes and store them
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var changeType = reader["SYS_CHANGE_OPERATION"].ToString(); // 'I', 'U', or 'D'
                        var employeeId = reader["Id"].ToString(); // Get employee ID
                        long sysChangeVersion = (long)reader["SYS_CHANGE_VERSION"]; // Get the version
                        byte[] changedColumn = reader["SYS_CHANGE_COLUMNS"] == DBNull.Value ? null : (byte[])reader["SYS_CHANGE_COLUMNS"];
                        changes.Add((changeType, employeeId, sysChangeVersion, changedColumn));
                    }
                }

                long maxVersionProcessed = lastSyncVersion; // Start with the current sync version

                foreach (var change in changes)
                {
                    if (change.changeType == "D")
                    {
                        // Handle delete operation
                        await SendToKafka(change.changeType, change.employeeId, "", "");
                        
                    }
                    else
                    {
                        string employeeDataQuery = BuildEmployeeDataQuery(change.changedColumn, columnNames);

                        using (SqlCommand fetchCommand = new SqlCommand(employeeDataQuery, conn))
                        {
                            fetchCommand.Parameters.AddWithValue("@Id", change.employeeId);

                            try
                            {
                                var data = await fetchCommand.ExecuteReaderAsync();

                                if (data != null && data.Read())
                                {
                                    string dataString = ConstructDataString(data, change.changedColumn, columnNames);
                                    // Send data to Kafka
                                    await SendToKafka(change.changeType, change.employeeId, dataString, string.Join(" ", columnNames));
                                   

                                }
                            }
                            catch (SqlException ex)
                            {
                                // Improved error logging
                                MessageBox.Show($"SQL Error: {ex.Message}\nQuery: {employeeDataQuery}");
                            }
                        }
                    }

                    maxVersionProcessed = Math.Max(maxVersionProcessed, change.sysChangeVersion);
                }

                // Store the new max version to prevent reprocessing
                UpdateLastSyncVersion(maxVersionProcessed);
            }
        }

        // Helper method to build the query based on changed columns
        private string BuildEmployeeDataQuery(byte[] changedColumn, List<string> columnNames)
        {
            if (changedColumn == null && columnNames.Count==0 || columnNames.Count == 0)
            {
                // No specific columns changed or SYS_CHANGE_COLUMNS is null, fetch all columns
                return "SELECT * FROM dbo.Employees WHERE Id = @Id";
            }
            else
            {
                // Columns have changed, build query to select only changed columns
                return $"SELECT {string.Join(", ", columnNames)} FROM dbo.Employees WHERE Id = @Id";
            }
        }

        // Helper method to construct data string
        private string ConstructDataString(SqlDataReader data, byte[] changedColumn, List<string> columnNames)
        {
            string dataString = "";

            if (changedColumn == null && columnNames.Count==0 || columnNames.Count == 0)
            {
                // Fetch all columns if no specific changes are detected
                for (int i = 0; i < data.FieldCount; i++)
                {
                    string columnName = data.GetName(i);
                    object columnValue = data[i];

                    // Append each column name and value to the dataString
                    dataString += $"{columnValue ?? ""} ";
                }
            }
            else
            {
                // Fetch only the changed columns and add them to the data string
                foreach (string column in columnNames)
                {
                    object columnValue = data[column];
                    string value = string.IsNullOrWhiteSpace(columnValue?.ToString()) ? "" : columnValue.ToString().Trim();
                    dataString += $"{value} ";
                }
            }

            return dataString;
        }

        //Send the changes to kafka

        private async Task SendToKafka(string changeType, string employeeId, string employeeData,string columnChange)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                if (columnChange == "")
                {
                    columnChange = "*";
                }
                string messageValue = $"{changeType}: {employeeId},changed columns: {columnChange}, Data: {employeeData}";

                try
                {
                    var result = await producer.ProduceAsync("Replication-topic", new Message<Null, string> { Value = messageValue });
                    //Console.WriteLine($"Message sent to partition {result.Partition}, offset {result.Offset}");
                    stopwatch.Stop();
                    MessageBox.Show($"Elapsed Time to send : {stopwatch.Elapsed.TotalSeconds} seconds");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
               
                using (SqlConnection connection = new SqlConnection(connString))
            {
                    try
                    {
                        connection.Open();
                        //MessageBox.Show("Database connection opened successfully.", "Info");
                        using (SqlCommand command = new SqlCommand("ALTER TABLE Employees DISABLE CHANGE_TRACKING;", connection))
                        {

                            command.ExecuteNonQuery();
                        }
                    using (SqlCommand command = new SqlCommand("ALTER TABLE Employees ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);", connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                    catch (SqlException sqlEx)
                    {
                        MessageBox.Show($"SQL error{sqlEx.Message}", "SQL Error");
                    }
                }

        }



        //retrives the changes from kfaka

        public async Task ConsumeKafkaMessage()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var config = new ConsumerConfig
            {
                GroupId = "ReplicatioonDB",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true, // Automatically commit offsets after consuming
            };
            using (var consumer = new ConsumerBuilder<Null, string>(config).Build())
            {
                consumer.Subscribe("Replication-topic");

                string lastMessage = null; // Store the last message
                ConsumeResult<Null, string> consumeResult;
                ConsumeResult<Null, string> lastConsumeRes=null;
                try
                {
                    
                    // Loop to consume messages until there are no more available
                    while (true)
                    {
                        consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500)); // Use a timeout

                        // Break the loop if there are no more messages
                        if (consumeResult == null)
                            break;
                        lastConsumeRes = consumeResult;
                        lastMessage = consumeResult.Message.Value; // Keep track of the last message

                        if (lastMessage != null)
                        {
                            // Process the last message
                            // Split the message by commas
                            var messageParts = lastMessage.Split(',');

                            // Extract the action and employee ID
                            var actionPart = messageParts[0].Trim(); // e.g., 'U: 1'
                            var changedColumnsPart = messageParts[1].Trim(); // e.g., 'changed columns: Name Salary'
                            var dataPart = messageParts[2].Trim(); // e.g., 'Data: Emp0001 50'

                            // Extract the change type and employee ID
                            var actionDetails = actionPart.Split(':');
                            var changeType = actionDetails[0].Trim(); // 'U'
                            var employeeId = actionDetails[1].Trim(); // '1'

                            // Extract changed columns
                            var changedColumns = changedColumnsPart.Replace("changed columns:", "").Trim().Split(' ');

                            // Extract data
                            var dataValues = dataPart.Replace("Data:", "").Trim().Split(' ');


                            // Update the second database based on change type

                            await ApplyChangesToDatabase(changeType, employeeId, dataValues, changedColumns);

                            
                            consumer.Commit(lastConsumeRes);
                        }
                        else
                        {
                            // Handle the case where no messages are found
                            MessageBox.Show("No messages found in Kafka topic.");
                        }
                    }
                    stopwatch.Stop();
                    MessageBox.Show($"Elapsed Time to retrieve \r\n: {stopwatch.Elapsed.TotalSeconds} seconds");
                }

                   
                catch (ConsumeException e)
                {

                    // Handle Kafka consumption errors
                    MessageBox.Show($"Error consuming Kafka message: {e.Error.Reason}");
                }
            }
           
        }
        private async Task ApplyChangesToDatabase(string changeType, string employeeId, string[] changedData, string[] changedColumns)
        {
            string connectionString = mirrorConString; // Connection string to the second database

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                SqlCommand command = null;

                switch (changeType)
                {
                    case "I": // Insert
                        string insertQuery = "INSERT INTO MirrorForEmployees (Id, ";
                        insertQuery += string.Join(", ", changedColumns) + ")"; // Dynamically add columns
                        insertQuery += " VALUES (@Id, " + string.Join(", ", changedColumns.Select(c => $"@{c}")) + ")";

                        command = new SqlCommand(insertQuery, conn);
                        command.Parameters.AddWithValue("@Id", employeeId);

                        // Add the dynamic columns to the insert query
                        for (int i = 0; i < changedColumns.Length; i++)
                        {
                            command.Parameters.AddWithValue($"@{changedColumns[i]}", changedData[i]);
                        }
                        break;

                    case "U": // Update
                        string updateQuery = "UPDATE MirrorForEmployees SET ";
                        List<string> setClauses = new List<string>();

                        foreach (string column in changedColumns)
                        {
                            setClauses.Add($"{column} = @{column}");
                        }

                        updateQuery += string.Join(", ", setClauses);
                        updateQuery += " WHERE Id = @Id";

                        command = new SqlCommand(updateQuery, conn);
                        command.Parameters.AddWithValue("@Id", employeeId);

                        // Add the dynamic columns to the update query
                        for (int i = 0; i < changedColumns.Length; i++)
                        {
                            command.Parameters.AddWithValue($"@{changedColumns[i]}", changedData[i]);
                        }
                        break;

                    case "D": // Delete
                        string deleteQuery = "DELETE FROM MirrorForEmployees WHERE Id = @Id";
                        command = new SqlCommand(deleteQuery, conn);
                        command.Parameters.AddWithValue("@Id", employeeId);
                        break;

                    default:
                        return;
                }

                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Database operation failed: {ex.Message}");
                }
            }
        }


    }
}
