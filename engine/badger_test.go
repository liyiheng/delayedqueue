package engine

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
)

func TestBadgerBasic(t *testing.T) {
	name, err := ioutil.TempDir("", "badger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(name)
	engine, err := NewWithBadger(badger.DefaultOptions(name))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	queue1 := []byte("queue_name1")
	queue2 := []byte("queue_name2")

	hello1 := []byte("hello1")
	hello2 := []byte("hello2")

	engine.Push(queue1, hello1, time.Second)
	engine.Push(queue2, hello2, time.Second)

	{

		elements1, err1 := engine.Pop(queue1, 100)
		elements2, err2 := engine.Pop(queue2, 100)
		if err1 != nil {
			t.Fatal(err1)
		}
		if err2 != nil {
			t.Fatal(err2)
		}
		if len(elements1) > 0 {
			t.Fatal("Elements1 should be empty")
		}
		if len(elements2) > 0 {
			t.Fatal("Elements2 should be empty")
		}
	}
	time.Sleep(time.Second)
	{

		elements1, err1 := engine.Pop(queue1, 100)
		elements2, err2 := engine.Pop(queue2, 100)
		if err1 != nil {
			t.Fatal(err1)
		}
		if err2 != nil {
			t.Fatal(err2)
		}
		if len(elements1) != 1 {
			t.Fatal("Elements1 should not be empty")
		}
		if len(elements2) != 1 {
			t.Fatal("Elements2 should not be empty")
		}
		eq1 := bytes.Equal(hello1, elements1[0])
		eq2 := bytes.Equal(hello2, elements2[0])
		if !eq1 || !eq2 {
			t.Fatal("Wrong data")
		}
	}

}
