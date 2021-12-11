A simple usage example of hashicorp/memberlist

---
#### API

- /add?key={somekey}&val={someval}
- /del?key={somekey}
- /get?key={somekey}

You don't want to have an API for *updating*

#### Deploy in Kubernetes
```shell
kubectl apply -f deploy/
```
