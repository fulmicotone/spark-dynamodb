package com.fulmicotone.dynamodb.write.function.impl;


import com.fulmicotone.dynamodb.common.DDBField;
import com.fulmicotone.dynamodb.write.exception.DDBWritableItemCreationException;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.List;

public  abstract class DDBSerializer  implements PairFunction<Row, Text, DynamoDBItemWritable> {

    protected abstract List<DDBField> fieldMap();

    @Override
    public Tuple2<Text, DynamoDBItemWritable> call(Row row)  throws Exception {
        try {
            return new Tuple2<>(new Text(""), new RowToDDBWritableItem().serialize(row,fieldMap()));
        } catch (DDBWritableItemCreationException e) {
            e.printStackTrace();
            throw  new Exception(e);
        }
    }
}
