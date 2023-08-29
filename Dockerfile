
FROM golang:1.21 as builder

# Copy the local package files to the container's workspace.
ADD . /go/src/github.com/antinvestor/service-ledger

WORKDIR /go/src/github.com/antinvestor/service-ledger

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /ledger_service .

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /ledger_service /ledger_service
COPY --from=builder /go/src/github.com/antinvestor/service-ledger/migrations /migrations
WORKDIR /

# Run the ledger command by default when the container starts.
ENTRYPOINT ["/ledger_service"]

# Document that the service listens on port 7000 by default.
EXPOSE 7000