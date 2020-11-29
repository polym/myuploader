FROM golang:1.15

ADD . /app
RUN cd /app && go build -o myuploader .

FROM ubuntu
COPY --from=0 /app/myuploader /app/myuploader
