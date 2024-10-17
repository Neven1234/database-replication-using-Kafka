using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaIntegration.Data
{
    public class ColumnChange
    {
        Dictionary<long, string> columnBitmaskMap = new Dictionary<long, string>()
        {
            { 2, "Name" },    // Bitmask for Name column 8589934592
            { 3, "Salary" },  // Bitmask for Salary column 17179869184
        };

        // get list of changed columns
        public List<string> GetChangedColumns(byte[] changedColumnsBytes, int totalColumns)
        {
            for (int i = 3; i <= 100; i++)
            {
                columnBitmaskMap.Add(i, "calumn" + i);
            }
            List<string> changedColumns = new List<string>();

            // Convert the byte array to a 64-bit integer
            long changedColumnBitmask = BitConverter.ToInt64(changedColumnsBytes, 0);

            // Iterate over the columns
            for (int columnIndex = 0; columnIndex <= totalColumns; columnIndex++)
            {
                // Adjusted to check correctly based on column position
                long bitmask = 1L << (columnIndex + 30); // This should match your representation of bits

                // Check if the corresponding bit is set
                if ((changedColumnBitmask & bitmask) != 0)
                {
                    // Ensure we only add columns based on correct mapping
                    if (columnBitmaskMap.TryGetValue(columnIndex+1 , out var columnName)) // Adjust for your keys
                    {
                        changedColumns.Add(columnName);
                    }
                }
            }

            // Output the changed columns
            Console.WriteLine("Changed Columns: " + string.Join(", ", changedColumns));

            return changedColumns;

        }
    }
}
