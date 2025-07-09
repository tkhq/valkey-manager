FROM stagex/pallet-go AS build

WORKDIR /src

COPY . .

RUN go build -o /app

FROM scratch AS package

COPY --from=build /app /app

ENTRYPOINT ["/app"]
