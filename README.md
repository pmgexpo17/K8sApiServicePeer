# K8sApiServicePeer
Fork of ApiServicePeer for Kubernetes conversion
<br>
Saas prototype for csv to json bi-conversion
<p>
Kubernetes commands to create a single cluster service
<p>
1. start minikube : minikube start
<br>
2. create persistant volume : kubectl create -f pv-volume.yaml
<br>
3. create pv claim : kubectl create -f pv-claim.yaml
<br>
4. create pv-pod : kubectl create -f pv-pod.yaml
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
    
