package pl.billennium.bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class HBaseTraining {
    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws
            IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
            System.out.println("Removed existing table [" + table.getNameAsString() +
                    "]");
        }
        admin.createTable(table);
        System.out.println("Table created [" + table.getNameAsString() + "]");
    }
    public static void createNamespace(Admin admin, String namespace) throws
            IOException {
        NamespaceDescriptor desc = admin.getNamespaceDescriptor(namespace);
        if (desc == null) {
            desc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(desc);
        }
    }
    public static void createSchemaTables(Configuration config, String namespace,
                                          String tableName, List<String> columnFamilies) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            createNamespace(admin, namespace);
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(namespace,
                    tableName));
            columnFamilies.forEach(c -> {
                table.addFamily(new
                        HColumnDescriptor(c).setCompressionType(Compression.Algorithm.NONE));
            });
            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }
    public static void insertData(Configuration config, String namespace, String
            tableName, String rowkey, String columnFamily, Map<String, String> data) throws
            IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Put p = new Put(Bytes.toBytes(rowkey));
            data.forEach((col, v) -> {
                p.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(col),
                        Bytes.toBytes(v));
            });
            Table table = connection.getTable(TableName.valueOf(namespace,
                    tableName));
            table.put(p);
            table.close();
        }
    }
    public static void findAndPrint(Configuration config, String namespace, String
            tableName, String keyPrefix,
                                    String cf, String qualifier, String value, String
                                            qualifierToPrint, int limit) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Scan scanner = new Scan();
            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filters.addFilter(new PrefixFilter(Bytes.toBytes(keyPrefix)));
            filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(cf),
                    Bytes.toBytes(qualifier), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value)));
            filters.addFilter(new PageFilter(limit));
            scanner.setFilter(filters);
            scanner.setCaching(limit);
            Table table = connection.getTable(TableName.valueOf(namespace,
                    tableName));
            ResultScanner results = table.getScanner(scanner);
            Iterator<Result> iter = results.iterator();
            int count = 0;
            System.out.println("Scanning table [" + tableName + "]");
            System.out.println("Results:");
            while(iter.hasNext() && count < limit) {
                count++;
                Result r = iter.next();
                System.out.println(Bytes.toString(r.getValue(Bytes.toBytes(cf),
                        Bytes.toBytes(qualifierToPrint))));
            }
            table.close();
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        List<String> columnFamilies = Arrays.asList("metadata", "content");
        String tableName = "tweets";
        String namespace = "mszczesny_twitter";
        //createSchemaTables(config, namespace, tableName, columnFamilies);
        Map<String, String> metaData = new HashMap<String, String>();
        Map<String, String> content = new HashMap<String, String>();

        String csv = args[0];
        BufferedReader br = null;
        String delimiter = ",";
        String line = "";
        String rowKey = "";

        try {

            br = new BufferedReader(new FileReader(csv));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] fields = line.split(delimiter);
                metaData.put("polar", fields[0]);
                metaData.put("id", fields[1]);
                metaData.put("c_date", fields[2]);
                metaData.put("owner", fields[4]);
                metaData.put("recipient", fields[3]);
                rowKey = fields[1];

                insertData(config, namespace, tableName, rowKey, "metadata", metaData);

                content.put("txt", fields[5]);
                insertData(config, namespace, tableName, rowKey, "content", content);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

