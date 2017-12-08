/*
Copyright 2017 gopool Author. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Golang Coroutines encapsulation, implementation coroutine pool.
对Golang的协程封装，实现协程池.
*/
package copool

import (
	"fmt"
	"time"

	"github.com/aosen/gopool"
)

const (
	DEFAULT_ADDR_CNT       = 256
	DEFAULT_ADDR           = "copool"
	DEFAULT_SIZE           = 256
	DEFAULT_COROUTINE_LIFE = 0
)

var cp *copool

func init() {
	initCopool()
}

type copool struct {
	pool *gopool.ChanConnPool
}

func create(addr string, timeout time.Duration) (cor interface{}, err error) {
	return newCoroutine(), nil
}

func isOpen(c interface{}) bool {
	return true
}

func down(c interface{}) {
	c.(*coroutine).closeCoroutine()
}

func setMaxCoroutineCnt(cnt int) (err error) {
	cp.pool, err = gopool.NewChanConnPool(&gopool.ConnPoolReq{
		Addrs: func() (addrs []string) {
			addrs = make([]string, DEFAULT_ADDR_CNT)
			for i := 0; i < DEFAULT_ADDR_CNT; i++ {
				addrs[i] = fmt.Sprintf("%s_%d", DEFAULT_ADDR, i)
			}
			return
		}(),
		Size:     cnt,
		Create:   create,
		IsOpen:   isOpen,
		Down:     down,
		ConnLife: DEFAULT_COROUTINE_LIFE,
	})
	return
}

func initCopool() {
	cp = &copool{}
	setMaxCoroutineCnt(DEFAULT_SIZE)
}

func (c *copool) get() (cor *coroutine) {
	corer, _ := c.pool.Get()
	cor = corer.(*coroutine)
	return
}

func (c *copool) put(cor *coroutine) {
	c.pool.Put(cor, true)
}

type task struct {
	fn   func(interface{})
	args interface{}
}

func newTask(fn func(interface{}), args interface{}) (t *task) {
	t = &task{
		fn:   fn,
		args: args,
	}
	return
}

type coroutine struct {
	ch chan *task
}

func newCoroutine() (cor *coroutine) {
	cor = &coroutine{
		ch: make(chan *task, 1),
	}
	go cor.run()
	return
}

func (cor *coroutine) run() {
	for {
		task, ok := <-cor.ch
		if !ok {
			break
		}
		task.fn(task.args)
		cp.put(cor)
	}
}

func (cor *coroutine) supergo(f func(interface{}), args interface{}) {
	cor.ch <- newTask(f, args)
}

func (cor *coroutine) closeCoroutine() {
	close(cor.ch)
}

func SetMaxCoroutineCnt(cnt int) error {
	return setMaxCoroutineCnt(cnt)
}

func Go(fn func(interface{}), args interface{}) {
	cp.get().supergo(fn, args)
}

func GetCoroutineCnt() int {
	return cp.pool.GetConnAllCount()
}
