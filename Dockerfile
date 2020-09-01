
FROM golang:1.10 as builder

# Copy the local package files to the container's workspace.
ADD . /go/src/bitbucket.org/caricah/service-ledger

#build go migrate
RUN go get -v -d github.com/golang-migrate/migrate/cli && go get -v -d github.com/lib/pq

WORKDIR /go/src/github.com/golang-migrate/migrate

RUN go build -tags 'postgres' -o /go/src/bitbucket.org/caricah/service-ledger/migrate_binary ./cli

WORKDIR /go/src/bitbucket.org/caricah/service-ledger


# Build the Ledger command inside the container.
RUN go install bitbucket.org/caricah/service-ledger

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o ledger_binary .

FROM scratch
COPY --from=builder /go/src/bitbucket.org/caricah/service-ledger/ledger_binary /ledger
COPY --from=builder /go/src/bitbucket.org/caricah/service-ledger/migrate_binary /migrate
COPY --from=builder /go/src/bitbucket.org/caricah/service-ledger/migrations /
WORKDIR /

# Run the ledger command by default when the container starts.
ENTRYPOINT migrate -path ./migrations -database postgres://postgres:postgres@localhost:5432/ur?sslmode=disable up
 && /ledger

# Document that the service listens on port 7000 by default.
EXPOSE 7000