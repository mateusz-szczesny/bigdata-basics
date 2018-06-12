package pl.billennium.bigdata.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TwitterHDFSUploader {
    public static void main(String ... args) {
        Path localSource = new Path(args[0]);
        Path dstSource = new Path(args[1]);

        try{
            Configuration conf = new Configuration();
            conf.set("fs.default.name", "hdfs://billhdp01.training.dev:8020");
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            System.out.println(conf.get("FS_PARAM_NAME") + "   " + conf.get("fs.defaultFS"));

            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(localSource, dstSource);

            System.out.println(dstSource + " transferred to HDFS successfully");
        } catch (Exception e){
            System.out.println("Aborted!");
            System.out.println(e.getMessage());
        }
    }
}
