FROM node:latest

RUN echo "America/Chicago" > /etc/timezone
RUN dpkg-reconfigure -f noninteractive tzdata
ARG VERSION
ENV VERSION ${VERSION}

ENV REDIS_HOST redis

WORKDIR /container

ADD ./package.json /container/
ADD ./package-lock.json /container/
RUN echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > ".npmrc"
RUN npm install
ADD . /container
CMD ["npm", "run", "start:cg"]
