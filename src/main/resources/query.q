PREFIX : <http://example.com/>

CREATE EVENT ?a  {
      subClassOf ( :detectedSmokeLevel min 0.6 )
}


CREATE EVENT ?b  {
      subClassOf ( :detectedSmokeLevel min 0.6 )
}

MATCH every (?a or ?d) or ?b
