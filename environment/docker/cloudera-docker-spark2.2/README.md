# Cloudera-docker-spark2.2

The version of Spark in the original Cloudera image is still 1.6.1.

This image upgrade it to 2.2 by hacking methods, did not promise 100% compatible with CDH.

## usage

### docker container

`docker build -t cdh-spark:2.2 .`
`docker run --hostname=quickstart.cloudera --privileged=true -ti --rm cloudera/quickstart /bin/bash`

### docker compose

`docker-compose build`
`docker-compose up`


