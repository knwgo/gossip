FROM golang:1.16 as builder
WORKDIR /workspace
COPY . .
RUN go mod download
RUN CGO_ENABLE=0 GOOS=linux GOARCH=amd64 go build -a -o gossip .

FROM alpine:latest
WORKDIR /
COPY --from=builder /workspace/gossip .
ENTRYPOINT ["/gossip"]