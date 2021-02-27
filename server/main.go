package main

import (
	"delayedqueue/engine"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/dgraph-io/badger/v3"
	"github.com/tidwall/redcon"
)

type configuration struct {
	Port   uint16        `toml:"port"`
	Engine string        `toml:"engine"`
	Badger *configBadger `toml:"badger"`
}

type configBadger struct {
	Path string `toml:"path"`
}

var cfgDefault = &configuration{Port: 6379,
	Engine: "badger",
	Badger: &configBadger{Path: "./queue_data"},
}

var queue engine.Interface

func main() {
	var cfg configuration
	{
		c := flag.String("c", "config.toml", "Configuration file")
		flag.Parse()
		_, err := toml.DecodeFile(*c, &cfg)
		if err != nil {
			log.Println(err)
			log.Println("Using default configs")
			cfg = *cfgDefault
		}
	}
	q, e := engine.NewWithBadger(badger.DefaultOptions(cfg.Badger.Path))
	if e != nil {
		panic(e)
	}
	queue = q
	defer queue.Close()
	go log.Printf("Listen port %d", cfg.Port)
	err := redcon.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), handler,
		func(conn redcon.Conn) bool {
			log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

func handler(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "llen":
		if len(cmd.Args) < 2 {
			conn.WriteInt(0)
			return
		}
		l, err := queue.Len(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(l)
	case "sadd":
		argc := len(cmd.Args)
		if argc < 2 {
			conn.WriteError("ERR queue name missing")
			return
		}
		if argc < 3 {
			conn.WriteError("ERR element and delay duration missing")
			return
		}
		if argc < 4 {
			conn.WriteError("ERR delay milliseconds missing")
			return
		}
		milliSec, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
		if err != nil {
			conn.WriteError("ERR invalid delay duraiton:" + err.Error())
			return
		}
		n := time.Now()
		err = queue.Push(cmd.Args[1], cmd.Args[2], time.Millisecond*time.Duration(milliSec))
		log.Println("sadd", time.Now().Sub(n))
		if err != nil {
			conn.WriteError(err.Error())
		} else {
			conn.WriteInt(1)
		}
	case "spop":
		if len(cmd.Args) < 2 {
			conn.WriteError("queue name missing")
			return
		}
		cnt := 1
		if len(cmd.Args) == 3 {
			c, err := strconv.Atoi(string(cmd.Args[2]))
			if err != nil {
				conn.WriteError("invalid count:" + err.Error())
				return
			}
			cnt = c
		}
		n := time.Now()
		elements, err := queue.Pop(cmd.Args[1], cnt)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		log.Println("spop", time.Now().Sub(n))
		conn.WriteArray(len(elements))
		for _, ele := range elements {
			conn.WriteBulk(ele)
		}
	case "keys":
		keys, err := queue.Queues()
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulk(k)
		}
	case "del":
		if len(cmd.Args) < 2 {
			conn.WriteError("queue name missing")
			return
		}
		// TODO
	}
}
