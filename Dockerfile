FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/server .

FROM alpine:3.24.4
COPY --from=build /bin/server /bin/server
EXPOSE 8080
ENTRYPOINT ["/bin/server"]
