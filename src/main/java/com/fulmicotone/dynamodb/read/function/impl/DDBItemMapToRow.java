package com.fulmicotone.dynamodb.read.function.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fulmicotone.dynamodb.read.exception.DDBDeserializationException;
import com.fulmicotone.dynamodb.read.function.IDDBItemMapToSparkRow;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.*;


public class DDBItemMapToRow implements IDDBItemMapToSparkRow<Map<String,AttributeValue>,StructType,Row> {


     @Override
     public Row deserialize(Map<String, AttributeValue> ddbItemMap, StructType structType) throws DDBDeserializationException {



               List<Object> fields=new ArrayList<>();

               Arrays.asList(structType.fields()).forEach(field->{

                    if(ddbItemMap.get(field.name())!=null ) {

                         if(field.dataType().sameType(createArrayType(StringType))) {

                              fields.add(ddbItemMap.get(field.name()).getL()
                                      .stream()
                                      .map(av -> av.getS()).collect(Collectors.toList()).toArray());
                         }
                         else if (field.dataType().sameType(LongType)) {

                              fields.add(Long.parseLong(ddbItemMap.get(field.name()).getN()));

                         } else if (field.dataType().sameType(IntegerType)) {

                              fields.add(Integer.parseInt(ddbItemMap.get(field.name()).getN()));

                         } else if (field.dataType().sameType(StringType)) {

                              fields.add(ddbItemMap.get(field.name()).getS());

                         } else if (field.dataType().sameType(BooleanType)) {

                              fields.add(ddbItemMap.get(field.name()).getBOOL());
                         }

                    }else{

                         fields.add(null);
                    }
               });

          return  new GenericRowWithSchema(fields.toArray(),structType);

     }
}
