
FROM golang:alpine as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o ledger .

FROM scratch
COPY --from=builder /build/ledger /
COPY --from=builder /build/migrations /
WORKDIR /

# Run the ledger command by default when the container starts.
ENTRYPOINT /ledger

# Document that the service listens on port 7000 by default.
EXPOSE 7000