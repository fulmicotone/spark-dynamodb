package com.fulmicotone.dynamodb;

import com.fulmicotone.dynamodb.read.business.DDBDeserializer;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FilmDeserializer extends DDBDeserializer {


    @Override
    public StructType structType() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Film", DataTypes.StringType,false),
                DataTypes.createStructField("Genre", DataTypes.StringType,false)
        });
    }



}
