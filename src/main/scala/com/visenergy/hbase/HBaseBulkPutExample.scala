package com.visenergy.hbase

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf
import com.cloudera.spark.hbase.HBaseContext

object HBaseBulkPutExample {
    def main(args: Array[String]) {
        val tableName = "image_table";
        val columnFamily = "Image Data"

        val sparkConf = new SparkConf().setAppName("HBaseBulkPutExample " + tableName + " " + columnFamily)
        val sc = new SparkContext(sparkConf)

        val rdd = sc.parallelize(Array(
            (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
            (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
            (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
            (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
            (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))
            )
        )

        val conf = HBaseConfiguration.create();
        conf.addResource(new Path("/eds/servers//hbase-1.0.1.1/conf/hbase-site.xml"));

        val hbaseContext = new HBaseContext(sc, conf);
        hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
           tableName,
           (putRecord) => {
               val put = new Put(putRecord._1)
               putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
               put
            },
            true);
    }
}