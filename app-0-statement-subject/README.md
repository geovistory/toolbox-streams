# Create docker image and push to local context 

Create the image
```bash
gradle build \
    -Dquarkus.package.type=native \
    -Dquarkus.container-image.push=false \
    -Dquarkus.container-image.registry="ghcr.io" \
    -Dquarkus.container-image.group="geovistory" \
    -Dquarkus.container-image.name="app-statement-subject" \
    -Dquarkus.container-image.tag="0.1.0"
```

Find the image
```
docker images

# should print something like:
# 
```

Run the image

```bash
docker run -p 8080:8080 --env-file=.env --platform=linux/amd64 ghcr.io/geovistory/toolbox-streams-statement-subject-quarkus:<VERSION> 
```
# Run .jar
java -jar build/quarkus-app/quarkus-run.jar

# Run public docker image

docker run -p 8080:8080 --env-file=.env --platform=linux/amd64 ghcr.io/geovistory/toolbox-streams-statement-subject-quarkus:<VERSION> 