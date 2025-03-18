# Fogverse Example

## Running
- cd examples/fogverse
- Create virtual env for fogverse example
- Install requirements
- Get bootstrap server address, see operator readme.
- Run producer
```
set PRODUCER_SERVERS=<bootstrap-server-addr> & python producer.py  
```
- Run consumer in seperate terminal, dont forget to activate the virtual env
```
set CONSUMER_SERVERS=<bootstrap-server-addr> & python consumer.py
```