build(){
    echo "Production build"
    sbt pack
    docker build --tag dgingestingest:latest .
    docker tag  dgingestingest asia.gcr.io/dataglen-prod-196710/dgingestingest
    docker push asia.gcr.io/dataglen-prod-196710/dgingestingest
}

deploy() {
  kubectl run batchingest --image=asia.gcr.io/atria-ee-platform/batchingest:latest 
}

build
