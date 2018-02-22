package com.fulmicotone.dynamodb.write.function.impl;

import com.fulmicotone.dynamodb.common.DDBField;
import com.fulmicotone.dynamodb.common.SparkTo;
import com.fulmicotone.dynamodb.write.exception.DDBWritableItemCreationException;
import com.fulmicotone.dynamodb.write.function.IRowToDDBItemMapper;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.stream.Collectors;

public class RowToDDBWritableItem implements IRowToDDBItemMapper<Row,List<DDBField>,DynamoDBItemWritable> {

    @Override
    public DynamoDBItemWritable serialize(Row row, List<DDBField> list) throws DDBWritableItemCreationException {

        DynamoDBItemWritable ddbItem;
        try {
                ddbItem=new DynamoDBItemWritable();

                 ddbItem.setItem(list.stream()
                    .collect(Collectors
                            .toMap((ddbField) -> ddbField.fieldName,
                                    (ddbField) -> SparkTo.ddb().attr(ddbField.fieldName, row, ddbField.typeClazz))));
        }catch (Exception e){
            throw  new DDBWritableItemCreationException(e.toString());
        }

        return ddbItem;
    }
}
