using KafkaIntegration.Data;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace KafkaIntegration
{
    public partial class InsertRow : Form
    {
        private DbHelper dbHelper;
        public InsertRow()
        {
            InitializeComponent();

        }

        private void button1_Click(object sender, EventArgs e)
        {
            dbHelper = new DbHelper();
            string employName = textBox1.Text;
            string employSalary = textBox2.Text;
            dbHelper.AddRecord(employName, employSalary);
            this.DialogResult = DialogResult.OK; 
            this.Close();
        }
    }
}
