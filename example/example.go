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

Based on the channel of connection pool
基于channel的连接池
*/
package main

import (
	"errors"
	"net"
	"time"
)

var addrs []string = []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}

var epool Pooler

func InitExpPool() (err error) {
	if epool == nil {
		epool, err = NewChanConnPool(&ConnPoolReq{
			Addrs: addrs,
			Create: func(addr string, timeout time.Duration) (interface{}, error) {
				cli, err := net.DialTimeout("tcp", addr, timeout)
				return cli, err
			},
			IsOpen: func(cli interface{}) bool {
				if cli != nil {
					return true
				}
				return false
			},
			Down: func(cli interface{}) {
				c := cli.(net.Conn)
				c.Close()
			},
		})
		return
	}
	return
}

func Get() (cli net.Conn, err error) {
	if epool == nil {
		err = errors.New("no init epool.")
		return
	}
	cli, err = epool.Get()
	return
}

func Put(cli net.Conn, safe bool) {
	if epool == nil {
		err := errors.New("no init epool.")
		return
	}
	epool.Put(cli, safe)
}

func GetHealthy() map[string]bool {
	if epool == nil {
		return nil
	}
	return epool.GetHealthy()
}

func GetConnCount() map[string]int {
	if epool == nil {
		return nil
	}
	return epool.GetConnCount()
}
