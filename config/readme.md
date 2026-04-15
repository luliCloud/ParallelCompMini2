### Single Node vs Multi Node
Single node need to change config file to read all dataset but not sharding data.

### Run the code
1. Run server C (if we already change yaml file)
```bash
./build/bin/server C
```
2. Run client
```bash
./build/bin/client -s localhost:50053 query --request-id baseline-all
```