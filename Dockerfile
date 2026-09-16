FROM scratch

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/scicat-cli /usr/bin/

ENTRYPOINT ["/usr/bin/scicat-cli"]
