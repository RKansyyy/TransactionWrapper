import javax.swing.*;
import javax.xml.transform.Result;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Wrapper implements Runnable{

    static Connection con=null;

    public int op;
    public boolean running;
    public double performance;
    public int operationsPerformed;
    private int numOp;
    private ArrayList<String> tableNames;

    private ResultSet resCustomer;
    private ResultSet resProduct;
    private Gui wow;
    private String ip;

    public Wrapper(Gui wow) {

        this.wow = wow;
        this.running = true;
        this.performance = 0;
        this.operationsPerformed = 0;
        this.numOp = 20;
        this.tableNames = getSchemaTables();
        setOperation();
        if(op == 0) getData();

    }

    public void run() {

        while(running) {

            switch(op) {
                case 0:
                    insert(numOp);
                    break;

                case 1:
                    delete(numOp);
                    break;

                case 2:
                    update(numOp);
                    break;

                case 3:
                    select(numOp);
                    break;

                default:
                    break;

            }
            operationsPerformed += op == 0 ? 3*numOp : numOp;

            SwingUtilities.invokeLater(() -> {
                wow.refreshPerformance();
            });
        }
    }

    private void delete(int deletes) {

        try {

            Statement st = getConnection().createStatement();

            int randomTable;

            // Generate an integer based on the number of tables
            randomTable = getRandomInRange(0, tableNames.size());
            // Get random table
            String[] schemaTable = tableNames.get(randomTable).split("\\.");

            String getColumnNames = "SELECT COLUMN_NAME " +
                                    "FROM INFORMATION_SCHEMA.COLUMNS " +
                                    "WHERE TABLE_NAME = N'" + schemaTable[1] + "'";

            // Get names of columns of the chosen table
            ResultSet res = st.executeQuery(getColumnNames);
            res.next();
            // Assuming the first column of a table is the index, this string will be the index
            String index = res.getString(1);

            long diff = 0;
            for(int j = 0; j < deletes; j++) {

                if(!running) break;
                // Delete randomly chosen row

                String randomValue = "Select top 1 " + index + " from " + schemaTable[0] + "." + schemaTable[1] + " Order by newid()";

                ResultSet resRand = st.executeQuery(randomValue);
                resRand.next();
                String delete = "Delete from " + schemaTable[0] + "." + schemaTable[1] +
                                " where " + index + " = " + resRand.getString(1);

                //System.out.println(delete);

                long pre = System.currentTimeMillis();

                st.execute(delete);

                long post = System.currentTimeMillis();
                diff += (post - pre);
            }

            performance = 1000 / (diff/(double)numOp);


        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    private void update(int updatesPerTable) {

        try {

            Statement st = getConnection().createStatement();

            int randomTable;
            int randomColumn;


            // Generate an integer based on the number of tables
            randomTable = getRandomInRange(0, tableNames.size() - 1);
            // Get random table
            String[] schemaTable = tableNames.get(randomTable).split("\\.");

            String getColumnNames = "SELECT COLUMN_NAME " +
                                    "FROM INFORMATION_SCHEMA.COLUMNS " +
                                    "WHERE TABLE_NAME = N'" + schemaTable[1] + "'";

            String rows = "SELECT Count(*) " +
                          "FROM INFORMATION_SCHEMA.COLUMNS " +
                          "WHERE TABLE_NAME = N'" + schemaTable[1] + "'";

            // get columns of random table
            ResultSet resCount = st.executeQuery(rows);
            resCount.next();

            //Choose random column to update by picking random between 2 and max number of rows
            randomColumn = getRandomInRange(2, Integer.valueOf(resCount.getString(1)) + 1);

            // Get names of columns of the chosen table
            ResultSet res = st.executeQuery(getColumnNames);
            String columnToUpdate = "";

            // Assuming the first column of a table is the index, this string will be the index
            String index = "";

            //TODO:
            // Fix while loop to not choose dependant columns...

            // Locate index Column and randomly chosen column
            while((randomColumn-- > 0 && res.next()) || res.getString(1).contains("id")) {
                if(index.equals("")) index = res.getString(1);
            }

            columnToUpdate = res.getString(1);

            double diff = 0;
            for(int j = 0; j < updatesPerTable; j++) {

                if(!running) break;

                String getRandomValue = "Select top 1 " + columnToUpdate + " from " + schemaTable[0] + "." + schemaTable[1] + " Order by newid()";

                String getRandomIndex = "Select top 1 " + index + " from " + schemaTable[0] + "." + schemaTable[1] + " Order by newid()";

                ResultSet resVal = st.executeQuery(getRandomValue);
                resVal.next();
                String randVal = resVal.getString(1);

                ResultSet resIndex = st.executeQuery(getRandomIndex);
                resIndex.next();
                String randIndex = resIndex.getString(1);

                // Update randomly chosen row with randomly chosen value out of the same column
                String update = "Update " + schemaTable[0] + "." + schemaTable[1] +
                                " Set " + columnToUpdate + " = " + "'" + randVal + "'" +
                                " where " + index + " = " + randIndex;

                long pre = System.currentTimeMillis();

                st.execute(update);

                long post = System.currentTimeMillis();
                diff += (post - pre);
            }

            performance = 1000 / (diff/(double)updatesPerTable);

        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    private void select(int selects) {

        try {

            Statement st = getConnection().createStatement();

            int randomTable;

            double diff = 0;
            for(int i = 0; i < selects; i++) {

                if(!running) break;
                randomTable = getRandomInRange(0, tableNames.size() - 1);

                String getRandomData = "Select * from " + tableNames.get(randomTable);

                long pre = System.currentTimeMillis();

                st.execute(getRandomData);

                long post = System.currentTimeMillis();
                diff += (post - pre);
            }

            performance = 1000 / (diff/(double)selects);

        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    private void insert(int inserts) {

        try {

            Statement stInsert = getConnection().createStatement();

            String first_name;
            String last_name;
            String productName;

            double diff = 0;
            int count = 0;

            while (resCustomer.next() && resProduct.next() && inserts-- > 0) {

                if(!running) break;

                first_name = resCustomer.getString(1);
                last_name = resCustomer.getString(2);

                if (!first_name.contains("'") && !last_name.contains("'")) {

                    String insertCustomer = "Insert into dbo.Customer(first_name, last_name) values" +
                                            "('" + first_name + "','" + last_name + "');";

                    long pre = System.currentTimeMillis();

                    stInsert.execute(insertCustomer);

                    long post = System.currentTimeMillis();
                    diff += (post - pre);
                    count++;

                }

                productName = resProduct.getString(1);

                if (!productName.contains("'")) {

                    String insertProduct = "Insert into dbo.Product(product_name, price) values" +
                                           "('" + productName + "','" + resProduct.getString(2) + "');";

                    stInsert.execute(insertProduct);

                    String insertSale = "Insert into dbo.sales(cid, pid, sale_amount) values " +
                                        "(" + "(SELECT TOP 1 cid FROM dbo.Customer ORDER BY NEWID())" +
                                        "," + "(SELECT TOP 1 pid FROM dbo.Product ORDER BY NEWID())" + "," + getRandomInRange(1,10) + ");";

                    stInsert.execute(insertSale);
                }

            }
            performance = 1000 / (diff/(double)count);

            if(!resProduct.next() || !resCustomer.next()) {

                resCustomer.beforeFirst();
                resProduct.beforeFirst();
            }

        } catch (Exception e){
            e.printStackTrace();

        }
    }

    public static Connection getConnection() {

        if (con != null) return con;
        // get db, user, pass from settings file
        return getConnection("172.17.1.27", "TestDB", "TestUser", "Test123");
    }

    private static Connection getConnection(String serverIp, String dbName, String userName, String password) {

        try {
            con = DriverManager.getConnection("jdbc:sqlserver://" + serverIp + ":1433;databaseName=" + dbName + ";user=" + userName + ";password=" + password);

        } catch(Exception e) {
            e.printStackTrace();
        }
        return con;
    }

    /**
     * A function to return the schemas and tables of the connected database
     * @return an arraylist filled with schemas and tables separated by a "." e.g dbo.customer
     */
    private ArrayList<String> getSchemaTables() {

        try {

            Statement st = getConnection().createStatement();

            String getTables = "SELECT" +
                    " * " +
                    "FROM " +
                    "INFORMATION_SCHEMA.TABLES;";

            ResultSet res = st.executeQuery(getTables);

            ArrayList<String> schemaTableNames = new ArrayList<>();

            while(res.next()) {

                schemaTableNames.add(res.getString(2) + "." + res.getString(3));

            }
            schemaTableNames.remove(0);

            return schemaTableNames;

        } catch (Exception e) {

            e.printStackTrace();
        }
        return null;
    }

    private void setOperation() {

        boolean[] mode = {true, true, true, true, true};

        try {

            Statement st = getConnection().createStatement();

            String connTableExists =
                    "IF (NOT EXISTS (SELECT * " +
                            "                FROM tempdb.sys.objects" +
                            "                WHERE name = '##wrapperConnections'))" +
                            "BEGIN" +
                            " create table ##wrapperConnections(" +
                            "op int not null, " +
                            "ip nvarchar(100)," +
                            "opPerSec dec(10,2)," +
                            "opPerformed int); " +
                            "END";

            st.execute(connTableExists);

            String sp = "Select op from ##wrapperConnections";

            ResultSet res = st.executeQuery(sp);

            int count = 4;

            while(res.next() && count-- > 0)
                mode[Integer.valueOf(res.getString(1))] = false;


            try(final DatagramSocket socket = new DatagramSocket()){
                socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                ip = socket.getLocalAddress().getHostAddress();
            }

            for(int i = 0; i < 5; i++) {

                if(mode[i]) {

                    op = i;
                    String insertCon = "Insert into ##wrapperConnections(op, ip, opPerSec, opPerformed) values(" +
                            i +
                            ", '" + ip + "'" +
                            ", " + performance +
                            ", " + operationsPerformed +
                            ")";

                    if(op < 4)
                        st.execute(insertCon);
                    break;
                }
            }

            if(op == 4) {
                op = getRandomInRange(0, 3);
                String insertCon = "Insert into ##wrapperConnections(op) values(" + op + ")";
                st.execute(insertCon);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void getData() {

        try {

            Connection conSource = DriverManager.getConnection("jdbc:sqlserver://172.17.1.27:1433;databaseName=AdventureWorks;user=TestUser;password=Test123");

            Statement st1 = conSource.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Statement st2 = conSource.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            String getPerson = "Select FirstName, LastName from Person.Person;";
            String getProduct = "Select Name, ListPrice from Production.Product where ListPrice > 0;";

            resCustomer = st1.executeQuery(getPerson);
            resProduct = st2.executeQuery(getProduct);

        } catch (Exception e){
            e.printStackTrace();

        }
    }

    public void closeConnection() {

        try {

            Statement st = getConnection().createStatement();
            String delConnection = "Delete from ##wrapperConnections where op = " + op + ";";
            st.execute(delConnection);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * Returns a random number in a specified range
     *
     * @param min Min value that can be returned by function
     * @param max Max value that can be returned by function
     * @return a random number between min and max
     */
    private int getRandomInRange(int min, int max) {

        return ThreadLocalRandom.current().nextInt(min, max);
    }
}
