# Delayed Queue
[README\_en](./README_en.md)

轻量延迟队列，使用 Redis(`REdis Serialization Protocal`) 协议，可通过 Redis 客户端使用

## 动机

曾使用 Rabbitmq 的延迟队列插件，数据量达百万后性能急剧下降，占用大量内存。
未发现基于磁盘的实现，本项目因此诞生。


## 使用示例

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
		// 通过 sadd 命令发送消息, 延迟时长单位为毫秒
		// sadd 队列名 消息体 延迟时长
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
			// 通过 spop 命令消费消息，若没有到期消息，返回空
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

## 具体使用

1. 编译: `go build -o delayedqueue ./server`
2. 配置，配置文件示例位于 `config.toml`:
```toml
port = 6379
engine = "badger"

[badger]
path = "./queue_data"
```
3. 运行 `./delayedqueue -c config.toml`
4. 通过 Redis 客户端连接
```sh
redis-cli
```
5. 发送延迟消息:
```plaintext
> sadd my_queue data_delays_10_seconds 10000
> sadd my_queue data_delays_1_seconds 1000
> sadd my_queue data_delays_5_seconds 5000
```
6. 消费消息:
```plaintext
# sleep 6 seconds
> spop my_queue 10
1) "data_delays_1_seconds"
2) "data_delays_5_seconds"
# sleep 4 seconds
> spop my_queue 10
1) "data_delays_10_seconds"
```

获取队列列表:
```
> keys
1) my_queue
```

## TODOs
- [ ] 单元测试
- [x] 基于 Badger 的实现
- [ ] 基于 TiKV 的实现
- [x] 生产消息: `sadd`
- [ ] 批量生产消息: `sadd`
- [x] 消费消息: `spop`
- [x] 获取队列列表: `keys`
- [ ] 删除队列: `del`
- [ ] 查询队列长度: `scard`
- [ ] 接入 Prometheus
- [ ] 接入 gops

## 已知问题

1. 消费消息时，spop 操作最差情况下耗时达 200ms，
批量消费的 `量` 过小会影响吞吐量。
2. 目前没有类似 Kaflka 、 Rabbitmq 等队列消费成功后 `提交` 的机制，
消息最多消费一次，消费过程失败的情况需要消费者自行处理。
