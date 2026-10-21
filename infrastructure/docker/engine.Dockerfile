FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ make cmake libpq-dev nlohmann-json3-dev ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY CMakeLists.txt ./
COPY services ./services
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF && cmake --build build -j2

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /source/build/taskforge-scheduler /source/build/taskforge-worker ./
USER 10001:10001
CMD ["/app/taskforge-worker"]
