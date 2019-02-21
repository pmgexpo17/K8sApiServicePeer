# K8sConverterSaas
Fork of ApiServicePeer for Kubernetes conversion
<br>
Saas prototype for csv to json bi-conversion, latest code update : 15/01/2019 
<p>
Docker commands for CICD :
<p>
# docker build command, --no-cache for updates, Dockerfile exists in cwd
<br>
docker build --no-cache -t apipeer:v1.0 .
<br>
# docker run container command, plus bind mount the log volume to easily inspect logs
<br>
docker run --name=apiPeer1 -d -v /path/to/app/log5000p1/app/log:/app/log -p 5000:5000 apipeer:v1.0
<br>
docker run --name=apiPeer2 -d -v /path/to/app/log5050p2/app/log:/app/log -p 5050:5000 apipeer:v1.0
<br>
# create a network to simulate clustered service execution
<br>
# peer1 is the client, peer2 is the service
<br>
# peer2 is the producer, peer1 is the consumer
<br>
docker network create apiNet1
<br>
docker network connect apiNet1 apiPeer1
<br>
docker network connect apiNet1 apiPeer2
<br>
docker inspect apiNet1 > apiNet1Log
<br>
# inspect apiNet1Log to find the ip address for each container
<br>
# now update the event config file, adding each ip address to the json job content
<br>
# eg, see config/saasEvent.json
<br>
# testing example, copy an csv gzipfile to the related saas repository
<br> 
docker cp /path/to/local/testdir/181226185843.tar.gz \
      apiPeer1:/app/volume/volbankAE/bankAA/loansBB/profitA1/auditAA/finAnlysA1
<br>
# run a curl api command to start the process
<br>
curl -X POST http://localhost:5000/api/v1/smart \ 
      -d 'job={"type":"director","id":null,"service":"csvxform.csvToJsonClient:CsvToJson"}' -d @temp/pmeta1.json
<br>
# testing example, how to update an apiservice module without stopping the container
<br>
# first, copy the updated code source to the container module directory
<br>  
docker cp /path/to/local/app/apiservice/csvxform/csvToJsonClient.py apiPeer1:/app/apiservice/csvxform
<br>
# then, run a command to update the module. This method also works for helper modules, eg, apitools
<br>
# where the service name is the source of the module that imports the helper module
<br>  
docker exec -t apiPeer1 python apiAgent.py csvToJson -r apiservice.csvxform.csvToJsonClient
<br>
# run a shell command on a container, eg, browse a session workspace
<br>
docker exec -ti apiPeer2 sh -c "ls /app/temp/190101015334"
<br>
# copy the output json file from the container to a local dir for inspection
<br>
docker cp apiPeer1:/app/volume/volbankAE/bankAA/auditAA/finAnlysA1/loansBB/profitA1/181226185843.json \ 
<br>
    /path/to/local/testdir
<br>
# run a command to build a new docker image version, use --no-cache
<br>
# note : for app content changes, the Dockerfile does change only the app content image
<br>
docker build --no-cache -t pmg7670/apipeer:v1.1 .
<br>
# now, push to DockerHub for remote pull availability
<br>
docker push pmg7670/apipeer:v1.1
<p>
Kubernetes commands to create a single cluster service
<p>
1. start minikube : minikube start
<br>
2. create persistant volume : kubectl create -f pv-volume.yaml
<br>
3. create pv claim : kubectl create -f pv-claim.yaml
<br>
4. create pv pod : kubectl create -f pv-pod.yaml
<br>
5. create service to expose pod on port 5000 : kubectl create -f service.yaml
<br>
6. get exposed service address : minikube service apipeer1 --url
<br>
7. test csvToJson k8s service : 
<br>
  - using returned ip_address:port service address, ping service
<br>
  - curl ip_address:port/api/v1/ping
    
# - 21/02/2019 - For all enterprise project enquires : pmg7670@gmail.com
