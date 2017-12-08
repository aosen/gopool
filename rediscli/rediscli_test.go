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

redis client
*/
package rediscli

import (
	"log"
	"testing"
)

func TestRedisCli(t *testing.T) {
	var (
		reply string
		rint  int
		rlist []string
		count int64
	)
	rc, err := NewRedisCli(&Req{
		Addrs: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		log.Println("NewRedisCli err:", err.Error())
		return
	}
	err = rc.Set("test", "helloworld")
	if err != nil {
		log.Println("Set err:", err.Error())
		return
	}
	err = rc.Set("test", "helloworld", "EX", 10)
	if err != nil {
		log.Println("Set err:", err.Error())
		return
	}
	reply, err = rc.Get("test")
	if err != nil {
		log.Println("Get err:", err.Error())
		return
	}
	log.Println("Get Val:", reply)
	err = rc.Del("test")
	if err != nil {
		log.Println("Del err:", err.Error())
		return
	}
	log.Println("del success.")
	err = rc.Setnx("test", "helloworld")
	if err != nil {
		log.Println("Setnx err:", err.Error())
		return
	}
	err = rc.Expire("test", 10)
	if err != nil {
		log.Println("Expire err:", err.Error())
		return
	}
	log.Println("Expire success.")
	rint, err = rc.Incr("test_incr")
	if err != nil {
		log.Println("Incr err:", err.Error())
		return
	}
	log.Println("Incr success.", rint)
	reply, err = rc.IncrByfloat("test_incr_by_float", 100)
	if err != nil {
		log.Println("IncrByfloat err:", err.Error())
		return
	}
	log.Println("IncrByfloat success.", reply)
	rint, err = rc.IncrBy("test_incrby", 12345)
	if err != nil {
		log.Println("IncrBy err:", err.Error())
		return
	}
	log.Println("IncrBy success.", rint)
	err = rc.SAdd("test_spop", "1234567890", "2345678901")
	if err != nil {
		log.Println("SAdd err:", err.Error())
		return
	}
	log.Println("SAdd success.")
	rlist, err = rc.SMembers("test_spop")
	if err != nil {
		log.Println("SMembers err:", err.Error())
		return
	}
	log.Println("SMembers success.", rlist)
	reply, err = rc.SPop("test_spop")
	if err != nil {
		log.Println("SPop err:", err.Error())
		return
	}
	log.Println("SPop success.", reply)
	slot := 0
	for {
		rlist, _, err = rc.SGets("spush")
		if err != nil {
			log.Println("sgets err:", err.Error())
			break
		}
		log.Println("sgets success.", rlist)
		slot++
	}
	log.Println("sgets list success.", slot)
	count, err = rc.SUnionstore("test_sunionstore", "test_spop")
	if err != nil {
		log.Println("SUnionstore err:", err.Error())
		return
	}
	log.Println("SUnionstore success.", count)
	log.Println("success.")
	err = rc.FlushAll()
	if err != nil {
		log.Println("FlushAll err:", err.Error())
		return
	}
	log.Println("FlushAll success.")
}
