# Test

# Start (in dev mode)

Dev mode runs against existing kafka brokers and schema registry.

Download dev-stack

```text
git clone https://github.com/geovistory/dev-stack.git

cd dev-stack

bash ./scripts/build

# wait until stack up and running
```

Start dev mode

```bash
# from this directory
quarkus dev
```

Open redpanda console from dev-stack and see topics:

http://localhost:1120/topics

At the same time, continuous testing is enabled. Hit 'd' in the terminal and navigate to Continuous Testing.
If you change (test-)code, the test will re-run.

# Test (continuous)

Tests launch redpanda using test containers. Only docker needed, no additional setup needed.

Start Continuous Testing

## With cli

Make sure to comment this line in application.properties:
`#quarkus.test.flat-class-path=true`

```bash
# from this directory
quarkus test
```

## With IDE

In IntelliJ navigate to the Test class and use the UI to start tests.
Make sure to uncomment this line in application.properties:
`quarkus.test.flat-class-path=true`
