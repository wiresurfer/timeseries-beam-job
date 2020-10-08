sbt pack
docker build --tag dgingestingest:latest .
docker stop dgingestingest 
docker rm dgingestingest
docker run --name dgingestingest --restart=always  dgingestingest:latest
