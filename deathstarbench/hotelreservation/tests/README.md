# Running deathstarbench on EKS

## Create EKS Cluster
First we need to create an EKS cluster.

```bash
eksctl create cluster -f eks-deathstar-cluster.yaml
```

Delete cluster with 
```bash
eksctl delete cluster -f eks-deathstar-cluster.yaml
```
