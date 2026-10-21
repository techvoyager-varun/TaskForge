FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends g++ make cmake libpq-dev nlohmann-json3-dev libgtest-dev ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build -j2
CMD ["npm","run","test:integration"]
