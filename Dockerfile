FROM eclipse-temurin:19

# install jemalloc
RUN apt update
RUN apt-get install -y libjemalloc-dev

ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "I am running on $BUILDPLATFORM, building for $TARGETPLATFORM"

# set LD_PRELOAD before you start your Kafka Streams application
# read https://kafka.apache.org/33/documentation/streams/developer-guide/memory-mgmt.html#id3
ENV LD_PRELOAD="/usr/lib/aarch64-linux-gnu/libjemalloc.so"
