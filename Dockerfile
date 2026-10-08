FROM gcr.io/distroless/static:nonroot

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/scicat-cli /usr/bin/

ENTRYPOINT ["/usr/bin/scicat-cli"]
