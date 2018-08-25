
FROM golang:1.10 as builder

# Copy the local package files to the container's workspace.
ADD . /go/src/bitbucket.org/caricah/service-ledger

WORKDIR /go/src/bitbucket.org/caricah/service-ledger

# Build the Ledger command inside the container.
RUN go install bitbucket.org/caricah/service-ledger

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o ledger_binary .

FROM scratch
COPY --from=builder /go/src/bitbucket.org/caricah/service-ledger/ledger_binary /ledger
COPY --from=builder /go/src/bitbucket.org/caricah/service-ledger/migrations /
WORKDIR /

# Run the ledger command by default when the container starts.
ENTRYPOINT /ledger

# Document that the service listens on port 7000 by default.
EXPOSE 7000