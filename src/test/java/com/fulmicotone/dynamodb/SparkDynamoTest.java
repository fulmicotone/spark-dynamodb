package com.fulmicotone.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fulmicotone.dynamodb.common.DDBField;
import com.fulmicotone.dynamodb.common.DDBJobConf;
import com.fulmicotone.dynamodb.read.DatasetDDBReader;
import com.fulmicotone.dynamodb.read.exception.DDBDeserializationException;
import com.fulmicotone.dynamodb.read.function.impl.DDBItemMapToRow;
import com.fulmicotone.dynamodb.write.DatasetDDBWriter;
import com.fulmicotone.dynamodb.write.exception.DDBWritableItemCreationException;
import com.fulmicotone.dynamodb.write.function.impl.RowToDDBWritableItem;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class SparkDynamoTest {


    /**
     * Function used to convert spark row to DDBWritableItem
     */
    @Test
    public void RowToDDBWritableItemTest()  {

        List<DDBField> fieldList = Arrays.asList(
                new DDBField("Film", StringType.class),
                new DDBField("Profitability",DecimalType.class),
                new DDBField("ActorsList",ArrayType.class));

       List<DynamoDBItemWritable> ddbWritableList= getSampleDataset().collectAsList().stream()
                .map(row->{
                    try {
                        return  new RowToDDBWritableItem().serialize(row,fieldList);
                    } catch (DDBWritableItemCreationException e) {
                        e.printStackTrace();
                        return null;
                    }
                }).collect(Collectors.toList());

        Assert.assertTrue(ddbWritableList.size()==77);
        DynamoDBItemWritable firstRow = ddbWritableList.get(0);
        Assert.assertTrue(firstRow.toString().equals("{\"Profitability\":{\"n\":\"1.747541667\"},\"ActorsList\":{\"l\":[\"a\",\"c\",\"f\",\"g\",\"h\"]},\"Film\":{\"s\":\"Zack and Miri Make a Porno\"}}"));

    }

    /**
     * this is used for creates the ddbJobConf directives for hadoop to write on dynamo
     */
    @Test
    public void DDBJobConfTest()  {

        String endpoint="https://dynamodb.us-west-2.amazonaws.com";
        String region="us-west-2";
        Float writePercent=0.8f;
        Integer writeThoughput=1;
        String outputTableName="filmDDBTable";
        Float readPercent=0.5f;
        Integer readThoughput=10;

        DDBJobConf ddbjob = DDBJobConf.Builder.newInstance()
                .setDdbRegion(region)
                .setWritePercent(writePercent)
                .setWriteThroughput(writeThoughput)
                .setOutputTableName(outputTableName)
                .setDdbEndpoint(endpoint)
                .setReadPercent(readPercent)
                .setReadThroughput(readThoughput)
                .create();

        Assert.assertTrue(ddbjob.get("dynamodb.output.tableName").equals(outputTableName));
        Assert.assertTrue(ddbjob.get("dynamodb.regionid").equals(region));
        Assert.assertTrue( ddbjob.get("dynamodb.throughput.write.percent").equals(String.valueOf(writePercent)));
        Assert.assertTrue(ddbjob.get("dynamodb.throughput.write").equals(String.valueOf(writeThoughput)));
        Assert.assertTrue(ddbjob.get("dynamodb.endpoint").equals(endpoint));
        Assert.assertTrue( ddbjob.get("dynamodb.throughput.read.percent").equals(String.valueOf(readPercent)));
        Assert.assertTrue(ddbjob.get("dynamodb.throughput.read").equals(String.valueOf(readThoughput)));

    }




    @Test
    public void DDBItemMapToRow() throws DDBDeserializationException {


        StructType filmRowStruct = DataTypes.createStructType(new StructField[]{


                DataTypes.createStructField("Film", DataTypes.StringType, false),
                DataTypes.createStructField("Genre", DataTypes.StringType, false),
                DataTypes.createStructField("Lead Studio", DataTypes.StringType, false),
                DataTypes.createStructField("Audience score %", DataTypes.IntegerType, false),
                DataTypes.createStructField("Profitability", DataTypes.createDecimalType(15, 9), false),
                DataTypes.createStructField("Actors", DataTypes.createArrayType(DataTypes.StringType), false)


        });



        HashMap<String,AttributeValue> map=new HashMap<>();

        AttributeValue titleValue = new AttributeValue();
        titleValue.setS("Zack and Miri Make a Porno");

        AttributeValue genreValue= new AttributeValue();
        genreValue.setS("Romance");

        AttributeValue leadStudioValue = new AttributeValue();
        leadStudioValue.setS("The Weinstein Company");

        AttributeValue scoreValue= new AttributeValue();
        scoreValue.setN("70");

        AttributeValue profitability= new AttributeValue();
        profitability.setN("1.747541667");


        AttributeValue actorsValue= new AttributeValue();
        actorsValue.setL(new ArrayList(Arrays.asList(new AttributeValue("a"),new AttributeValue("b"))));


        //Film,Genre,Lead Studio,Audience score %,Profitability,Rotten Tomatoes %,Worldwide Gross,Year,Actors

        map.put("Film",titleValue);
        map.put("Genre",genreValue);
        map.put("Lead Studio",leadStudioValue);
        map.put("Audience score %",scoreValue);
        map.put("Profitability",profitability);
        map.put("Actors",actorsValue);


        new DDBItemMapToRow().deserialize(map,filmRowStruct);







    }







    public static Dataset<Row> getSampleDataset(){


        StructType schema = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField("Film", DataTypes.StringType, false),
                        DataTypes.createStructField("Genre", DataTypes.StringType, false),
                        DataTypes.createStructField("Lead Studio", DataTypes.StringType, false),
                        DataTypes.createStructField("Audience score %", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Profitability", DataTypes.createDecimalType(15,9), false),
                        DataTypes.createStructField("Rotten Tomatoes %", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Worldwide Gross", DataTypes.StringType, false),
                        DataTypes.createStructField("Year", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Actors", DataTypes.StringType, false)


                });

        SparkSession session = SparkSession.builder().master("local").getOrCreate();
        return  session.read()
                .option("header", "true")
                .schema(schema).csv(SparkDynamoTest.class
                .getClassLoader()
                .getResource("film.csv")
                .getPath()).withColumn("ActorsList",split(col("Actors"),","));


    }



}
