# Delayed Queue

A lightweight delayed queue speaks REdis Serialization Protocal.


## Quick start
```go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

func sendMsgs() {
	client := redis.NewClient(&redis.Options{})
	defer client.Close()
	ctx := context.TODO()
	for j := 0; j < 1000; j++ {
		millis := rand.Int31n(1000 * 60)
		millis += 1000
		data := "Value " + time.Now().Add(time.Duration(millis)*time.Millisecond).String()
		client.SAdd(ctx, "queue_name", data, strconv.Itoa(int(millis)))
	}
}

func pollMsgs(done <-chan struct{}) {
	client := redis.NewClient(&redis.Options{})
	defer client.Close()
	ctx := context.TODO()
	batchSize := int64(1000)
	for {
		select {
		case <-done:
			break
		default:
			m := client.SPopN(ctx, "queue_name", batchSize)
			if redis.Nil == m.Err() || len(m.Val()) == 0 {
				time.Sleep(time.Millisecond * 300)
				continue
			}
			msgs := m.Val()
			for _, msg := range msgs {
				fmt.Println(msg)
			}
		}
	}
}
```

## Usage

1. Build it: `go build -o delayedqueue ./server`
2. Configure it, `config.toml`:
```toml
port = 6379
engine = "badger"

[badger]
path = "./queue_data"
```
3. Run it. `./delayedqueue -c config.toml`
4. Connect to it.
```sh
redis-cli
```
5. Produce msgs:
```plaintext
> sadd my_queue data_delays_10_seconds 10000
> sadd my_queue data_delays_1_seconds 1000
> sadd my_queue data_delays_5_seconds 5000
```
6. Comsume msgs:
```plaintext
# sleep 6 seconds
> spop my_queue 10
1) "data_delays_1_seconds"
2) "data_delays_5_seconds"
# sleep 4 seconds
> spop my_queue 10
1) "data_delays_10_seconds"
```

Get all queues:
```
> keys
1) my_queue
```

## TODOs
- [ ] Unit testing
- [x] Backend badger
- [ ] Backend TiKV
- [x] Produce msgs: `sadd`
- [ ] Produce batch msgs: `sadd`
- [x] Consume msgs: `spop`
- [x] Get all queue names: `keys`
- [ ] Delete a queue: `del`
- [ ] Queue length: `scard`
- [ ] Prometheus metrics
- [ ] Gops
