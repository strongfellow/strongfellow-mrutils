aws emr --region us-west-2 add-steps \
  --cluster-id j-JME5XQWSV0LY \
  --steps '[ 
  { 
    "Name": "crawl-baby-crawl", 
    "Args": [ "crawl",
              "--output=s3://emr.strongfellow.com/emr.strongfellow.com/crawl/outputs/2015-05-07", 
              "s3://emr.strongfellow.com/crawl-inputs/2015-05-07T21/input" 
            ],
    "Jar": "s3://emr.strongfellow.com/jars/mrutils-0.0.1-SNAPSHOT.jar", 
    "ActionOnFailure": "CONTINUE", 
    "Type": "CUSTOM_JAR" 
  } 
]'
