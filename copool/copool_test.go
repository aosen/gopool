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

copool
*/
package copool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const CNT = 10000

type Args struct {
	val int
	wg  *sync.WaitGroup
}

func do(args interface{}) {
	sum := 0
	it := args.(*Args)
	for i := it.val; i < it.val+100000; i++ {
		sum += i / 2 / 2
	}
	if it.wg != nil {
		it.wg.Done()
	}
	return
}

func TestSeq(t *testing.T) {
	now := time.Now().UnixNano()
	for i := 0; i < CNT; i++ {
		do(&Args{i, nil})
	}
	timeCost := (time.Now().UnixNano() - now) / int64(time.Nanosecond)
	fmt.Println("TestSeq cost:", timeCost, "ns")
}

func TestGo(t *testing.T) {
	wg := new(sync.WaitGroup)
	now := time.Now().UnixNano()
	for i := 0; i < CNT; i++ {
		wg.Add(1)
		go do(&Args{i, wg})
	}
	wg.Wait()
	timeCost := (time.Now().UnixNano() - now) / int64(time.Nanosecond)
	fmt.Println("TestGo cost:", timeCost, "ns")
}

func TestCopool(t *testing.T) {
	SetMaxCoroutineCnt(1)
	wg := new(sync.WaitGroup)
	now := time.Now().UnixNano()
	for i := 0; i < CNT; i++ {
		wg.Add(1)
		Go(do, &Args{i, wg})
	}
	wg.Wait()
	timeCost := (time.Now().UnixNano() - now) / int64(time.Nanosecond)
	fmt.Println("TestCopool cost:", timeCost, "ns", "Coroutines Cnt:", GetCoroutineCnt())
}
