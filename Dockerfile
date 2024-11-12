
FROM golang:1.23 AS builder


WORKDIR /

COPY go.mod .
COPY go.sum .
RUN go env -w GOFLAGS=-mod=mod && go mod download

# Copy the local package files to the container's workspace.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /ledger_service .

FROM gcr.io/distroless/static:nonroot

USER 65532:65532
EXPOSE 80
EXPOSE 50051

WORKDIR /

COPY --from=builder /ledger_service /ledger_service
COPY --from=builder /migrations /migrations

# Run the ledger command by default when the container starts.
ENTRYPOINT ["/ledger_service"]
