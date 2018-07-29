
FROM golang:alpine as builder

# Copy the local package files to the container's workspace.
ADD . /go/src/bitbucket.org/caricah/ledger

WORKDIR /go/src/bitbucket.org/caricah/ledger

# Build the Ledger command inside the container.
RUN go install bitbucket.org/caricah/ledger

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o ledger .

FROM scratch
COPY --from=builder /go/src/bitbucket.org/caricah/ledger/ledger /
COPY --from=builder /go/src/bitbucket.org/caricah/ledger/migrations /
WORKDIR /

# Run the ledger command by default when the container starts.
ENTRYPOINT /ledger

# Document that the service listens on port 7000 by default.
EXPOSE 7000