# Hotel Reservation Workload

Largely borrowed from the [hotel reservation workload in deathstarbench](https://github.com/delimitrou/DeathStarBench/tree/master/hotelReservation).

## Pre-requirements

- A running Kubernetes cluster is needed. To create a eks cluster:
  ```
  cd tests
  eksctl create cluster -f eks-deathstar-cluster.yaml
  ```

## Running the Hotel Reservation application

### Before you start

- Review the URL's embedded in `hr-client/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua` to be sure they are correct for your environment.
  The current value of `http://frontend.default.svc.cluster.local:5000` is valid for a typical "on-cluster" configuration.
- Build the hotel reservation client docker image using 
  ```
  cd hr-client
  ./docker_build_kind.sh # Or ./docker_build_ecr.sh, depending on cluster
  ```

### Deploy services

```
cd kubernetes
./launch.sh
```

Wait for `kubectl get pods` to show all pods with status `Running`.

### SSH into HTTP workload generator
```
hrclient=$(kubectl get pod | grep hr-client- | cut -f 1 -d " ")
kubectl exec -it $hrclient /bin/bash
cd /wrk2/
```

#### Running HTTP workload generator

##### Template
```bash
./wrk -D exp -R <reqs-per-sec> -t <num-threads> -c <num-conns> -d <duration> -L -s ./wrk2_lua_scripts/mixed-workload_type_1.lua http://frontend.default.svc.cluster.local:5000
```

##### Example
```bash
./wrk -R 1000 -D exp -t 16 -c 16 -d 30 -L -s ./scripts/hotel-reservation/mixed-workload_type_1.lua http://frontend.default.svc.cluster.local:5000 
```

### View Jaeger traces

Use `oc -n hotel-res get ep | grep jaeger-out` to get the location of jaeger service.

View Jaeger traces by accessing:
- `http://<jaeger-ip-address>:<jaeger-port>`  (off cluster)
- `http://jaeger.hotel-res.svc.cluster.local:6831`  (on cluster)


### Tips

- If you are running on-cluster, you can use the following command to copy files off of the client.
e.g., to copy the results directory from the on-cluster client to the local machine:
  - `hrclient=$(oc get pod | grep hr-client- | cut -f 1 -d " ")`
  - `oc cp hotel-res/${hrclient}:/root/DeathStarBench/hotelReservation/openshift/results /tmp`

### wrk2 usage
```console
Usage: wrk <options> <url>                                       
  Options:                                                       
    -c, --connections <N>  Connections to keep open              
    -D, --dist        <S>  fixed, exp, norm, zipf
    -P                     Print each request's latency
    -p                     Print 99th latency every 0.2s to file
    -p                     Print 99th latency every 0.2s to file
    -d, --duration    <T>  Duration of test
    -t, --threads     <N>  Number of threads to use

    -s, --script      <S>  Load Lua script file
    -H, --header      <H>  Add header to request
    -L  --latency          Print latency statistics
    -T  --timeout     <T>  Socket/request timeout
    -B, --batch_latency    Measure latency of whole
                           batches of pipelined ops
                           (as opposed to each op)
    -v, --version          Print version details
    -R, --rate        <T>  work rate (throughput)
                           in requests/sec (total)
                           [Required Parameter]


  Numeric arguments may include a SI unit (1k, 1M, 1G)
  Time arguments may include a time unit (2s, 2m, 2h)

```
