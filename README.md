# **Spark AWS Dynamo DB**
#### Library  for read or write Spark Dataset  from/to dynamo db



##### **How to read a Dataset from DynamoDB**

- Extends DDBSerializer, creating the structType 
- create The DDBJobConf indicating the dynamo db details
- Use DatasetDDBReader Function
 
 
 ##### _Film Dynamo Serializer Convert from Dynamo Item to Spark Row_
```
 public class FilmDeserializer extends DDBDeserializer {
 
 
     @Override
     public StructType structType() {
         return DataTypes.createStructType(new StructField[]{
                 DataTypes.createStructField("Film", DataTypes.StringType,false),
                 DataTypes.createStructField("Genre", DataTypes.StringType,false)
         });
     }
 
 }
 
```



##### _DDBJobConf_

``` 
 DDBJobConf ddbconf=DDBJobConf.Builder.newInstance()
                .setDdbRegion(region)
                .setWritePercent(writePercent)
                .setWriteThroughput(writeThoughput)
                .setOutputTableName(outputTableName)
                .setDdbEndpoint(endpoint)
                .create();
}
```
 
 
 ##### _Use read Function_
 ``` 
 Dataset<Row> dataset= new DatasetDDBReader().read(new FilmDeserializer(),ddbconf,session)
 }
 ```


##### **How to Write a Dataset from DynamoDB**

- Extends DDBSerializer and create the fieldList indicating the column Name and the 
the specific type.
- create The DDBJobConf indicating the dynamo db details
- read and pass the Dataset to the DatasetDDBWriter function
 


##### _Film Dynamo Serializer_

``` 

public class FilmSerializer extends DDBSerializer {
    @Override
    protected List<DDBField> fieldMap() {
          return Arrays.asList(
                new DDBField("Film", StringType.class),
                new DDBField("Genre", StringType.class),
                  new DDBField("Lead Studio", StringType.class),
                  new DDBField("Audience score %", IntegerType.class),
                  new DDBField("Profitability", DecimalType.class),
                  new DDBField("Rotten Tomatoes %", StringType.class),
                  new DDBField("Worldwide Gross", StringType.class),
                  new DDBField("Year", IntegerType.class),
                  new DDBField("Actors", StringType.class)

                  );

    }
}
```


##### _DDBJobConf_
``` 

 DDBJobConf ddbconf=DDBJobConf.Builder.newInstance()
                .setDdbRegion(region)
                .setWritePercent(writePercent)
                .setWriteThroughput(writeThoughput)
                .setOutputTableName(outputTableName)
                .setDdbEndpoint(endpoint)
                
                .create();
}
```


##### _Write Funciton_

``` 
session=SparkSession.builder().master("local").getOrCreate();

inputDataset=session.read.option("header","true").csv("./film.csv);

 new DatasetDDBWriter().write(ddbserializer,ddbconf,new FilmSerializer());
}

```




------


