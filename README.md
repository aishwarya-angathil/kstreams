# kstreams

Command to run code

In eclipse :

dependency:copy-dependencies clean compile assembly:single

Maven 

mvn dependency:copy-dependencies clean compile assembly:single



Command to run the jar file :

Producer :

java -jar kstreams-0.0.1-SNAPSHOT-jar-with-dependencies.jar dev.properties P


Compacted Producer Insert :

java -jar kstreams-0.0.1-SNAPSHOT-jar-with-dependencies.jar dev.properties CP I


Compacted Producer Update :

java -jar kstreams-0.0.1-SNAPSHOT-jar-with-dependencies.jar dev.properties CP U


Consumer :

java -jar kstreams-0.0.1-SNAPSHOT-jar-with-dependencies.jar dev.properties C

