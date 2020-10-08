FROM java:8

WORKDIR /usr/app
COPY ./beamsdk.json /usr/app
ENV GOOGLE_APPLICATION_CREDENTIALS /usr/app/beamsdk.json
COPY ./target/pack/ /usr/app/

CMD ["/bin/bash", "./bin/dg-aggregate-stream"]
