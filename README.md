# Flight App Project

This project is a Scala application that processes flight data using Apache Spark. It includes various operations like reading from and writing to CSV files, calculating flight counts per month, and identifying frequent flyers.

## Table of Contents

- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage]($usage)
- [Running Tests](#running-tests)

## Project Structure


## Prerequisites

- **Java:** This project was developed and tested using Java 1.8.
- **Scala:** The project uses Scala 2.12.10.
- **SBT:** The build tool used is SBT 1.10.11.
- **Apache Spark:** The project uses Apache Spark 2.4.8.
- **Docker** (optional).

## Setup

- For running the project make sure the tooling mentioned above with the proper version is installed on your local machine (Java, Scala, SBT, Apache Spark).
- Alternatively only Docker can be setup in order to run both test and the app itself from Docker.

## Usage

Case 1 - Run from the command line using the sbt-shell:
1. Open the <i>flight_app</i> folder.
2. Run `sbt` to open the sbt-shell.
3. In the sbt-shell type: `` run <flight csv file> <passengers.csv> [output path]  ``.
Eg: `run ./src/main/resources/flightData.csv ./src/main/resources/passengers.csv`. By default the `output` parameter corrsponding to the output folder, will be the `output` - i.e. the output data can be found under the parent folder in the `output` folder.

Case 2 - import the project in IntelliJ:
1. Open IntelliJ.
2. Import the project - remember to select the correct versions for Java and Scala.
3. Run the `Main` class using the run config - remember to specify the mandatory parameters.

Case 3 - Docker:
1. Open the <i>flight_app</i> folder.
2. Run `docker build -t scala-sbt-app .`
3. Run `docker run -v $(pwd)/src/main/resources:/app/src/main/resources -v $(pwd)/output:/app/output scala-sbt-app sbt "runMain Main /app/src/main/resources/flightData.csv /app/src/main/resources/passengers.csv /app/output/output.csv"`

## Running Tests
Case 1 - Run from the command line using the sbt-shell:
1. Open the <i>flight_app</i> folder.
2. Run `sbt` to open the sbt-shell.
3. In the sbt-shell type: `` test ``

Case 2 - import the project in IntelliJ:
1. Open IntelliJ.
2. Import the project - remember to select the correct versions for Java and Scala.
3. Run the test manually (or using run configs) from the `` ./src/test/scala ``

Case 3 - Docker:
1. Open the <i>flight_app</i> folder.
2. Run `docker build -t scala-sbt-app .`
3. Run `docker run scala-sbt-app `


docker run -v $(pwd)/src/main/resources:/app/src/main/resources -v <host-output-dir>:/app/output my-scala-app sbt "runMain Main /app/src/main/resources/flightData.csv /app/src/main/resources/passengers.csv /app/output/output.csv"
docker run -v $(pwd)/src/main/resources:/app/src/main/resources -v $(pwd)/output:/app/output my-scala-app sbt "runMain Main /app/src/main/resources/flightData.csv /app/src/main/resources/passengers.csv /app/output/output.csv"


docker run -v $(pwd)/src/main/resources:/app/src/main/resources -v $(pwd)/output:/app/output scala-sbt-app sbt "runMain Main /app/src/main/resources/flightData.csv /app/src/main/resources/passengers.csv /app/output/output.csv"
