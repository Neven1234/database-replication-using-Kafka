using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace KafkaIntegration.Data
{
    public class DbHelper
    {
        //the Publisher Db connection string
        private string PrimeString;
        private KafkaHelper kafkaHelper;

        // the subscribed Db connection string
        private string subscribedConnString;
        public DbHelper()
        {
            PrimeString = ConfigurationManager.ConnectionStrings["primerConStr"].ConnectionString;
            subscribedConnString = ConfigurationManager.ConnectionStrings["replicationServ"].ConnectionString;
        }

        //start cooection and retrive the data
        public DataTable PublisherDBConn(string query)
        {
            return StartConn(query, PrimeString);
        }
        public DataTable MirroringDbConn(string query)
        {
            return StartConn(query, subscribedConnString);
        }
        private DataTable StartConn(string query, string connString)
        {
            DataTable dataTable = new DataTable();
            using (SqlConnection connection = new SqlConnection(connString))
            {
                try
                {
                    connection.Open();
                    //MessageBox.Show("Database connection opened successfully.", "Info");
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {

                        using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                        {
                            adapter.Fill(dataTable);

                        }
                    }
                }
                catch (SqlException sqlEx)
                {
                    MessageBox.Show($"SQL error{sqlEx.Message}", "SQL Error");
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Failed to connect or execute query: {ex.Message}", "Error");
                }
            }
            return dataTable;
        }

        //Update primer table
        public async Task Update(DataRow updatedRow)
        {
            kafkaHelper = new KafkaHelper();
            using (SqlConnection connection = new SqlConnection(PrimeString))
            {
                try
                {
                    connection.Open();
                    List<string> columnsToUpdate = new List<string>();
                    List<string>columnNames=new List<string>();
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    // Loop through columns to dynamically detect changes
                    foreach (DataColumn column in updatedRow.Table.Columns)
                    {
                        string columnName = column.ColumnName;

                        if (updatedRow[columnName, DataRowVersion.Current] != DBNull.Value &&
                            !updatedRow[columnName, DataRowVersion.Current].Equals(updatedRow[columnName, DataRowVersion.Original]))
                        {
                            columnsToUpdate.Add($"{columnName} = @{columnName}");
                            columnNames.Add(columnName);
                            parameters.Add(new SqlParameter($"@{columnName}", updatedRow[columnName]));
                        }
                    }

                    if (columnsToUpdate.Count > 0)
                    {
                        // Build the update query dynamically
                        string updateQuery = $"UPDATE Employees SET {string.Join(", ", columnsToUpdate)} WHERE Id = @Id";
                        using (SqlCommand command = new SqlCommand(updateQuery, connection))
                        {
                            // Add parameters from the updated row
                            foreach (var parameter in parameters)
                            {
                                command.Parameters.Add(parameter);
                            }

                            // Add Id parameter
                            command.Parameters.AddWithValue("@Id", updatedRow["Id"]);

                            // Execute the update
                            command.ExecuteNonQuery();
                        }
                    }
                    await kafkaHelper.CheckForChanges(columnNames);                   
                    //// applay the changes to the mirroring db
                    
 
                }
                catch (SqlException sqlEx)
                {
                    MessageBox.Show($"SQL error: {sqlEx.Message}", "SQL Error");
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Failed to connect or execute query: {ex.Message}", "Error");
                }
            }
        }


        //Insert new Row
        public void AddRecord(string employName,string salary)
        {
            using(SqlConnection connection = new SqlConnection(PrimeString))
            {
                try
                {
                    connection.Open();
                    string query = "INSERT INTO Employees (Name,Salary) VALUES (@Name,@Salary);";
                    using (SqlCommand command = new SqlCommand(query,connection))
                    {
                        command.Parameters.AddWithValue("@Name", employName);
                        command.Parameters.AddWithValue("@Salary",salary);
                        command.ExecuteNonQuery();  
                    }
                }
                catch (SqlException sqlEx)
                {
                    MessageBox.Show($"SQL error{sqlEx.Message}", "SQL Error");
                }
            }
        }
    }
}
