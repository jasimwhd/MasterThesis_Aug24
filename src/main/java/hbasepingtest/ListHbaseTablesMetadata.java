package hbasepingtest;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ListHbaseTablesMetadata {
    public static List<String> getColumn(String my_db, String my_table) throws SQLException
    {
        try {
            Class.forName(MavenMainHbase.driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con1 = DriverManager
                .getConnection
                        ("jdbc:hive2://localhost:10000/"
                                        + my_db, "hive"
                                , "hive");
        System.out.println("hello");
        Statement stmt = con1.createStatement();

        ResultSet o_table = stmt.executeQuery
                ("describe " + my_table);

        ResultSetMetaData rsmd = o_table.getMetaData();

        List<String> o = new ArrayList<>();

        while (o_table.next())
        o.add(o_table.getString(rsmd.getColumnName(1)));

        return o;


    }
}
