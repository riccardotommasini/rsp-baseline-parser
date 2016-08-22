prefix :  <http://example.org/ns#> 

CREATE EVENT ?a  {
      subClassOf ( :detectedSmokeLevel min 0.6 )
}


CREATE EVENT ?b  {
      subClassOf ( :detectedSmokeLevel min 0.6 )
}

MATCH every { ?s :p :o } or ( ?b -> ?c )
