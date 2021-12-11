image := "ccr.ccs.tencentyun.com/kayn-infra/gossip:latest"

Go:
	docker build -t ${image} .
	docker push ${image}