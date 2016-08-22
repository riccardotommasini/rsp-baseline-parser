REGISTER QUERY ssdasdaf COMPUTED EVERY  1m AS

PREFIX : <http://streamreasoning.org/iminds/massif/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ssniot: <http://IBCNServices.github.io/Accio-Ontology/SSNiot#>
PREFIX ssn: <http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#>
PREFIX dul: <http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#>


SELECT *
FROM  WINDOW  [RANGE 1h, SLIDE 15m] ON STREAM :temperaturestr
FROM  WINDOW  [RANGE 1h, SLIDE 15m] ON STREAM :smokestr
FROM NAMED WINDOW :smk [RANGE 1h, SLIDE 15m] ON STREAM :smokestr

WHERE  {
    ?event ?p ?o ;
        ssniot:hasContext ?observation .

    ?observation ssn:observedProperty ?prop ;
        ssn:observedBy ?sensingDevice ;
        ssn:observationResult ?result .

    ?prop ?p2 ?o2.
    ?sensingDevice dul:hasLocation	?location .
    ?location dul:hasDataValue ?locationvalue.

    ?result ssn:hasValue ?value.
    ?value dul:hasDataValue ?v.

    WINDOW :tmp {
            ?s ?p ?o
    }



      WINDOW :smk {
                ?s ?p ?o

        }



    FILTER(?v > 90)
}
