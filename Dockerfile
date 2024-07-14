# Use an official OpenJDK runtime as a parent image
FROM openjdk:8-jdk-alpine

# Set environment variables for Scala and SBT versions
ENV SCALA_VERSION 2.12.10
ENV SBT_VERSION 1.10.1

# Install dependencies
RUN apk add --no-cache bash wget curl git

# Download and install Scala
RUN wget https://downloads.lightbend.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz && \
    tar xzf scala-$SCALA_VERSION.tgz && \
    mv scala-$SCALA_VERSION /usr/share/scala && \
    ln -s /usr/share/scala/bin/scala /usr/bin/scala && \
    ln -s /usr/share/scala/bin/scalac /usr/bin/scalac && \
    ln -s /usr/share/scala/bin/scaladoc /usr/bin/scaladoc && \
    ln -s /usr/share/scala/bin/scalap /usr/bin/scalap && \
    rm scala-$SCALA_VERSION.tgz

# Download and install SBT
RUN wget https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz && \
    tar xzf sbt-$SBT_VERSION.tgz && \
    mv sbt /usr/share/sbt && \
    ln -s /usr/share/sbt/bin/sbt /usr/bin/sbt && \
    rm sbt-$SBT_VERSION.tgz

# Set the working directory
WORKDIR /app

# Copy the project files to the container
COPY . /app

# Run sbt tests
CMD ["sbt", "test"]
