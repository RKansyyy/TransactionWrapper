import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;


class Gui extends JFrame {

    ArrayList<Wrapper> wrappers;
    ArrayList<Thread> threads;
    DefaultTableModel model;
    int selectedRow;
    boolean selected;
    static Connection con = null;


    public void refreshPerformance() {

        try {
            Statement st = getConnection().createStatement();
            String updateGui = "Select op, ip, opPerSec, opPerformed from ##wrapperConnections";

            ResultSet res = st.executeQuery(updateGui);


        } catch (Exception e) {
            e.printStackTrace();
        }

        for(int i = 0; i < wrappers.size(); i++) {

            model.setValueAt(new DecimalFormat("#.##").format(wrappers.get(i).performance), i, 1);
            model.setValueAt(new DecimalFormat("#.##").format(wrappers.get(i).operationsPerformed), i, 2);

        }
    }

    private void startThread() {

        if(getConnectedWrappers() < 4) {

            Wrapper w = new Wrapper(this);
            wrappers.add(w);
            Thread t = new Thread(wrappers.get(wrappers.size() - 1));
            t.start();
            threads.add(t);

            String s = "";

            switch (w.op) {

                case 0:
                    s = "Insert";
                    break;

                case 1:
                    s = "Delete";
                    break;

                case 2:
                    s = "Update";
                    break;

                case 3:
                    s = "Select";
                    break;
            }


            model.addRow(new Object[]{s, w.performance, w.operationsPerformed});
            System.out.println("Wrapper started with: " + s);

        } else {

            System.out.println("All operations running");
        }
    }

    private void stopThread() {

        if(selected) {

            Wrapper w = wrappers.get(selectedRow);
            Thread t = threads.get(selectedRow);
            w.running = false;
            model.removeRow(selectedRow);

            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
            }

            w.closeConnection();
            wrappers.remove(w);
            selected = false;

        } else {
            System.out.println("No operations running");

        }
    }

    public int getConnectedWrappers() {

        String getConnectionTable = "IF (EXISTS (SELECT * " +
                "                FROM tempdb.sys.objects" +
                "                WHERE name = '##wrapperConnections'))" +
                " BEGIN " +
                "Select Count(*) from ##wrapperConnections" +
                " END";

        try {

            Statement st = getConnection().createStatement();
            ResultSet res = st.executeQuery(getConnectionTable);
            res.next();

            int connections = Integer.valueOf(res.getString(1));

            return connections;


        } catch (Exception e) {
            return 0;
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

    private Gui() {

        threads = new ArrayList<>();
        wrappers = new ArrayList<>();
        model = new DefaultTableModel();

        model.addColumn("Type");
        model.addColumn("Operations/Sec");
        model.addColumn("Operations");


        JTable jt = new JTable(model);
        JScrollPane sp = new JScrollPane(jt);



        JPanel panel = new JPanel(); // the panel is not visible in output
        JButton start = new JButton("Start");
        JButton stop = new JButton("Stop");
        panel.add(start);

        panel.add(stop);

        start.addActionListener(e -> {
            startThread();
        });

        stop.addActionListener(e -> {

            stopThread();
        });

        jt.getSelectionModel().addListSelectionListener(e ->{

            selected = true;
            selectedRow = jt.getSelectedRow();

        });

        this.getContentPane().add(BorderLayout.NORTH, sp);
        this.getContentPane().add(BorderLayout.SOUTH, panel);

        this.setSize(500,600);
        this.setVisible(true);
        this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

    }

    public static void main(String args[]) {

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                Gui gui = new Gui();
                gui.setVisible(true);
            }
        });
    }


}