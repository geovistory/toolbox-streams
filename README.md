# toolbox-streams
Geovistory toolbox module for aggregating data about entities, implemented with Kafka Streams.

## Layout
```
root
|- app # topologies
|- lib # models and serdes
|- utilities # helpers for developers
```


## Run
### Requirements

- Kafka
- Schema Registry

### Environment

## Develop

### CLI
The

## Deploy

### App ID


### Base image used by jib

#### build base image

Set the correct version number

`docker build . -t ghcr.io/geovistory/kafka-streams-base:0.1`


#### push base image to ghcr.io


login to github container registry with docker cli (see (official docs)[https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry])

1. Create a new GitHub personal access token (classic) with write access to packages
2. Save your personal access token.
```bash
export CR_PAT=YOUR_TOKEN
```
3. Using the CLI for your container type, sign in to the Container registry service at ghcr.io.
```bash
echo $CR_PAT | docker login ghcr.io -u USERNAME --password-stdin
> Login Succeeded
```

Push image
```bash
docker push ghcr.io/geovistory/kafka-streams-base:0.1 
```


