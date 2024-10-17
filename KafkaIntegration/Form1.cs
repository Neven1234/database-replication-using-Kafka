using Confluent.Kafka;
using DevExpress.Xpo.DB.Helpers;
using DevExpress.XtraTreeList;
using KafkaIntegration.Data;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using static DevExpress.XtraEditors.Mask.MaskSettings;

namespace KafkaIntegration
{
    public partial class Form1 : Form
    {
        private DbHelper dbHelper;
        private DataTable dataTable;
        
        //private DataTable mirrorDataTable;
        private DataTable mirrorDataTable;

        private KafkaHelper kafkaHelper;
        public Form1()
        {
            InitializeComponent();


            LoadData();
            //kafkaHelper = new KafkaHelper();
            // kafkaHelper.ConsumeKafkaMessage();

        }
        public async Task LoadData()
        {
            string query = "SELECT * FROM Employees";
            string mirrorQuery = "SELECT * FROM MirrorForEmployees";
            dbHelper = new DbHelper();
            dataTable = dbHelper.PublisherDBConn(query);
            mirrorDataTable = dbHelper.MirroringDbConn(mirrorQuery);
             SetupTreeList(treeList1, dataTable);
             SetupTreeList(treeList2, mirrorDataTable);

        }
        //Add data to the tree list
        private void  SetupTreeList(TreeList treeList, DataTable dataTable)
        {
            treeList.BeginUpdate();
            try
            {
                treeList.DataSource = null;    // Clear current data            
                // Clear existing columns
                treeList.Columns.Clear();

                // Create and add columns
                treeList.DataSource = dataTable;
                treeList.KeyFieldName = "ID";
                treeList.ParentFieldName = "ParentID";
                treeList.PopulateColumns();
                // Bind data

            }
            finally
            {
                treeList.EndUpdate();
            }
        }

        private async void button1_Click(object sender, EventArgs e)
        {
           kafkaHelper = new KafkaHelper();
            // Iterate over each row in the DataTable
            foreach (DataRow row in dataTable.Rows)
            {
                // Check if the row state is modified
                if (row.RowState == DataRowState.Modified)
                {

                    // Update the row in the database
                   await dbHelper.Update( row);

                }

            }
            await kafkaHelper.ConsumeKafkaMessage();

            // Accept the changes to the DataTable
            dataTable.AcceptChanges();
            mirrorDataTable = dbHelper.MirroringDbConn("SELECT * FROM MirrorForEmployees");

            SetupTreeList(treeList2, mirrorDataTable);


        }

        private async void button2_Click(object sender, EventArgs e)
        {
            using (InsertRow addRowForm = new InsertRow())
            {
                if (addRowForm.ShowDialog() == DialogResult.OK)
                {
                    // Refresh the treeList1 to reflect the newly added data

                    kafkaHelper = new KafkaHelper();
                    List<string> addedColumns = new List<string>();
                    addedColumns.Add("Name");
                    addedColumns.Add("Salary");
                    await kafkaHelper.CheckForChanges(addedColumns);
                    // applay the changes to the mirroring db
                 
                    await kafkaHelper.ConsumeKafkaMessage();


                }
            }
        }
    }
}
