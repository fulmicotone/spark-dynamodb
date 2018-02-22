package com.fulmicotone.dynamodb.read;

import com.fulmicotone.dynamodb.common.DDBJobConf;
import com.fulmicotone.dynamodb.common.UtilsFunction;
import com.fulmicotone.dynamodb.read.business.DDBDeserializer;
import com.fulmicotone.dynamodb.read.exception.DDBReadException;
import com.fulmicotone.dynamodb.read.function.IDatasetDDBReader;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class DatasetDDBReader implements IDatasetDDBReader<DDBDeserializer,DDBJobConf, SparkSession,Dataset<Row>> {
    @Override
    public Dataset<Row> read(DDBDeserializer deserializer, DDBJobConf ddbJobConf, SparkSession session) throws DDBReadException {


        RDD<Tuple2<Text, DynamoDBItemWritable>> readFromDynamo = session.sparkContext()
                .hadoopRDD(ddbJobConf,
                        DynamoDBInputFormat.class,
                        Text.class,
                        DynamoDBItemWritable.class,
                        UtilsFunction.getParallelismFn.apply(session));

        //decoded tuple2 to havardd
        JavaRDD<Row> rowDecoded = readFromDynamo
                .toJavaRDD()
                .map((org.apache.spark.api.java
                        .function.Function<Tuple2<Text, DynamoDBItemWritable>, Row>) v1 ->
                        deserializer
                                .deserialize(v1._2.getItem()));
        //create dataframe
        return session.createDataFrame(rowDecoded, deserializer.structType());

    }


}
