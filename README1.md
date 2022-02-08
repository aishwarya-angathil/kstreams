i have created a pojo class for json
I have implemented JsonSerializer and JsonDeserializer classes for serializingand deserializing the variables of json pojo class..
I have added kstream in the consumer which adds serialized and deserialized serde into the input topic..
 While creating a  producerRedcord, I have not added any data  from the json pojo class.. I am still working on it..
 As of now , i have added null only producerRedcord
 I have added KStreamPractise  ,Person,JsonSerializer and JsonDeserializer classes newly..
 I have made changes in KStreamPractise based on KStreamApp class