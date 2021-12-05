FROM golang:1.16 as builder
WORKDIR /workspace
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64  go build -a -tags timetzdata -ldflags '-s' -o gossip .

FROM scratch
ENV TZ=Asia/Shanghai
COPY --from=builder /workspace/gossip .
ENTRYPOINT ["/gossip"]