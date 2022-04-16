 RAW TOPIC:-
 
for raw topic, placed the json msg in dev.properties file.
for json msg, created avro schema JsonSchema and as the eventtype is insert we have  placed the columns in after tag in raw topic with key as partyid and value as entire data of partyId columns

Compacted Topc1(for Individual data):-

placed the individual data in dev.properties in a variable json.compacted.i.1.
created avro schema Individual and placed it in compacted topic with key as partyId and value as entire data

Compacted Topc2(ExternalIdentifiers array):-

placed the ExternalIdentifiers array in dev.properties in a variable json.compacted.i.2.
I have created another compacted topic compactedTopic1 for ExternalIdentifiers
created avro schema External and placed it in compacted topic1  with key as partyId and value as entire data in the variable json.compacted.i.2.

JOiner Part(consumer):-
created externalSerde for ExternalIdentifiers in consumer.
created another KTable i.e., externalTble for ExternalIdentifiers
created the respective Enriched Files and joiners for JOing raw topic with compacted topic1 and raw topic with compacted topic2
KStream<String, Enriched2> dest = source.join(tble, joiner2);//for joining raw with compacted1
KStream<String, Enriched3> dest1 = source.join(externalTble, joiner);//for joining raw with compacted2
			 
tried to concatenate/Join the above two outputs like this.But, landing up in exception
KStream<String, Enriched4> finalOutput = dest.join(dest1, joiner3, JoinWindows.of(Duration.ofSeconds(0))); 

Placed the exception in group chat as well
First, i have tried to concatenate the outputs in another way.
But, as i am getting errors i have tried like this 
			
