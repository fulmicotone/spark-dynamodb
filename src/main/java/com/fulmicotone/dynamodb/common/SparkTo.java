package com.fulmicotone.dynamodb.common;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.slf4j.LoggerFactory;


public  class SparkTo {

    private org.slf4j.Logger log= LoggerFactory.getLogger(SparkTo.class);

    private static SparkTo INSTANCE=new SparkTo();

    private SparkTo(){}

    public static SparkTo ddb(){ return INSTANCE; }

  public  <E extends DataType> AttributeValue attr(String field, Row row, Class<E> type) {


      if (type.getSimpleName().equals(BooleanType.class.getSimpleName())) {
          AttributeValue attr = new AttributeValue();
          attr.setBOOL(row.getBoolean(row.fieldIndex(field)));
          return attr;
      } else

        if (type.getSimpleName().equals(LongType.class.getSimpleName())) {
            AttributeValue attr = new AttributeValue();
            attr.setN(String.valueOf(row.getLong(row.fieldIndex(field))));
            return attr;
        } else if (type.getSimpleName().equals(IntegerType.class.getSimpleName())) {
            AttributeValue attr = new AttributeValue();
            attr.setN(String.valueOf(row.getInt(row.fieldIndex(field))));
            return attr;
        }else if (type.getSimpleName().equals(DoubleType.class.getSimpleName())) {
            AttributeValue attr = new AttributeValue();
            attr.setN(String.valueOf(row.get(row.fieldIndex(field))));
            return attr;
        }
        else if (type.getSimpleName().equals(DecimalType.class.getSimpleName())) {

            AttributeValue attr = new AttributeValue();
            attr.setN(String.valueOf(row.getDecimal(row.fieldIndex(field))));
            return attr;
        }
        else if (type.getSimpleName().equals(ArrayType.class.getSimpleName())) {
            AttributeValue attr = new AttributeValue();
            attr.setL(row.getList(row.fieldIndex(field)));
            return attr;
        }
        else if (type.getSimpleName().equals(StringType.class.getSimpleName())) {
            AttributeValue attr =new AttributeValue();
              attr.setS(row.getString(row.fieldIndex(field)));
            return attr;
        }
        log.debug("type unknown, integrate the conversion allowed now: [Long,Int,String,List]");
        return null;
    }


}
