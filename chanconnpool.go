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
package gopool

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//Maximum error number has just been unfrozen connection
//刚被解冻的连接最大出错数
const MaxRetryAfterCooldown = 3

//downstream address property
//下游地址属性
type addrAttr struct {
	//Whether it is not healthy nodes
	//是否为不健康节点，
	health bool
	//After dangerous counter, when the counter reaches threshold will health set to false
	//危险计数器，当计数器达到阈值后将health置为false
	errcount int64
	//Cooling time
	//冷却时间点
	cooldowntime int64
	//Address the current total number of connections
	//地址当前总连接数
	conncount int32
}

//Connection properties
//连接属性
type connAttr struct {
	//Connect the corresponding address
	//连接对应的地址
	addr string
	//Whether in the pool of true or false is not, When higher concurrency value, considering the performance, the data may not be accurate, but in general is accurate
	//是否在池中 true 在 false 不在, 当并发量比较高时, 考虑到性能，此数据可能不太准，但大体来说是准确的
	inpool bool
	//Whether or not connected to borrow
	//是否为借的连接
	borrow bool
	//Life values, the second
	//生命值, 秒
	life int64
}

type ChanConnPool struct {
	r *rand.Rand
	//The properties of the state address list, corresponding address
	//地址列表，对应地址的属性状态
	addrs map[string]*addrAttr
	//The current legal address list
	//当前合法的地址列表
	al      []string
	alocker *sync.RWMutex
	//Threshold, the health to nodes threshold will be set to false unhealthy node
	//健康阈值，达到阈值将节点置为不健康节点 false
	healthyThreshold int64
	//The maximum cooling time
	//最大冷却时长
	maxCooldownTime int64
	//Than the minimum health, prevent all removed
	//最小健康比, 防止全部摘除
	minHealthyRatio float64
	//Connection timeout
	//连接超时时间
	connSvrTimeout time.Duration
	//Retry count
	//重试次数
	retryTimes int
	//Health check cycle
	//健康检查周期
	checkAddrCycle time.Duration
	//Connection with address mapping relationship
	//连接与地址的映射关系
	mapConn map[interface{}]*connAttr
	mlocker *sync.RWMutex
	//Create a client connection method
	//创建客户端连接方法
	create func(addr string, timeout time.Duration) (interface{}, error)
	//To determine whether a connection closed
	//判断连接是否关闭
	isOpen func(c interface{}) bool
	//Close the connection
	//关闭连接
	down func(c interface{})
	//The default maximum number of single connection error, error if more than this number will close the connection
	//默认最大单连接出错数，如果超过这个出错数就close这个连接
	maxConnBeFalseCnt int
	//Connection lifecycle
	//连接生命周期
	connLife int64
	//Each corresponding pool downstream nodes
	//每个下游节点对应的池子
	idle   map[string]chan interface{}
	locker *sync.Mutex
}

func NewChanConnPool(req *ConnPoolReq) (*ChanConnPool, error) {
	if len(req.Addrs) == 0 {
		return nil, errors.New("downstream address is empty.")
	}
	if req.Create == nil || req.IsOpen == nil || req.Down == nil {
		return nil, errors.New("no init create | isopen | down method")
	}
	cp := &ChanConnPool{
		r:                rand.New(rand.NewSource(time.Now().UnixNano())),
		addrs:            map[string]*addrAttr{},
		alocker:          new(sync.RWMutex),
		healthyThreshold: req.GetHealthyThreshold(),
		maxCooldownTime:  req.GetMaxCooldownTime(),
		minHealthyRatio:  req.GetMinHealthyRatio(),
		connSvrTimeout:   req.GetConnSvrTimeOut(),
		retryTimes:       req.GetRetryTimes(),
		checkAddrCycle:   req.GetCheckAddrCycle(),
		mapConn:          map[interface{}]*connAttr{},
		mlocker:          new(sync.RWMutex),
		create:           req.Create,
		isOpen:           req.IsOpen,
		down:             req.Down,
		connLife:         req.GetConnLife(),
		locker:           new(sync.Mutex),
	}
	for _, addr := range req.Addrs {
		cp.addAddr(addr)
	}
	cp.initIdle(req.GetSize())
	go cp.check()
	return cp, nil
}

//According to the link for the connection properties
//根据连接获取该连接的属性
func (pool *ChanConnPool) getMapConn(c interface{}) (string, bool, bool, bool) {
	pool.mlocker.RLock()
	defer pool.mlocker.RUnlock()
	if connattr, ok := pool.mapConn[c]; ok {
		return connattr.addr, connattr.inpool, connattr.borrow, ok
	} else {
		return "", false, false, false
	}
}

//To obtain the life value of the connection
//获取连接的生命值
func (pool *ChanConnPool) getConnLife(c interface{}) int64 {
	pool.mlocker.RLock()
	defer pool.mlocker.RUnlock()
	if connattr, ok := pool.mapConn[c]; ok {
		return connattr.life
	} else {
		return 0
	}
}

//Set the state of connections in the pool
//设置连接是否在池中的状态
func (pool *ChanConnPool) setMapConnState(c interface{}, in bool) {
	pool.mlocker.Lock()
	defer pool.mlocker.Unlock()
	if attr, ok := pool.mapConn[c]; ok {
		attr.inpool = in
	}
}

//Sets whether connect the corresponding address and connection to borrow
//设置连接对应的地址和是否为借的连接
func (pool *ChanConnPool) setMapConnAddr(c interface{}, addr string, borrow bool) {
	pool.mlocker.Lock()
	defer pool.mlocker.Unlock()
	if attr, ok := pool.mapConn[c]; !ok {
		pool.mapConn[c] = &connAttr{
			addr:   addr,
			inpool: false,
			borrow: borrow,
			life:   time.Now().Unix(),
		}
	} else {
		attr.borrow = borrow
	}
}

func (pool *ChanConnPool) deleteMapConn(c interface{}) {
	pool.mlocker.Lock()
	defer pool.mlocker.Unlock()
	delete(pool.mapConn, c)
}

func (pool *ChanConnPool) lenMapConn() int {
	pool.mlocker.RLock()
	defer pool.mlocker.RUnlock()
	l := len(pool.mapConn)
	return l
}

func (pool *ChanConnPool) getAddr(k string) bool {
	pool.alocker.RLock()
	defer pool.alocker.RUnlock()
	attr, ok := pool.addrs[k]
	if !ok {
		return ok
	}
	return attr.health
}

func (pool *ChanConnPool) getAllAddr() map[string]*addrAttr {
	pool.alocker.RLock()
	defer pool.alocker.RUnlock()
	addrs := pool.addrs
	return addrs
}

func (pool *ChanConnPool) addAddr(addr string) {
	pool.alocker.Lock()
	defer pool.alocker.Unlock()
	pool.addrs[addr] = &addrAttr{true, 0, int64(0), 0}
	if pool.al == nil {
		pool.al = []string{}
	}
	pool.al = append(pool.al, addr)
	sort.Strings(pool.al)
}

func (pool *ChanConnPool) tryIncAddrConnCount(addr string) bool {
	pool.locker.Lock()
	defer pool.locker.Unlock()
	if pool.getAddrConnCount(addr) < pool.capIdle(addr) {
		if attr, ok := pool.addrs[addr]; ok {
			atomic.AddInt32(&attr.conncount, 1)
		}
		return true
	}
	return false
}

func (pool *ChanConnPool) decAddrConnCount(addr string) {
	if attr, ok := pool.addrs[addr]; ok {
		atomic.AddInt32(&attr.conncount, -1)
	}
}

func (pool *ChanConnPool) getAddrConnCount(addr string) int {
	if attr, ok := pool.addrs[addr]; ok {
		return int(attr.conncount)
	}
	return 0
}

func (pool *ChanConnPool) setAddrdown(addr string) {
	pool.alocker.Lock()
	defer pool.alocker.Unlock()
	if attr, ok := pool.addrs[addr]; ok {
		attr.cooldowntime = time.Now().Unix()
		attr.health = false
		attr.errcount = 0
	}
	al := []string{}
	for k, v := range pool.addrs {
		if v.health {
			al = append(al, k)
		}
	}
	sort.Strings(al)
	pool.al = al
}

func (pool *ChanConnPool) incAddrUnSafeCount(addr string) {
	pool.alocker.Lock()
	defer pool.alocker.Unlock()
	if attr, ok := pool.addrs[addr]; ok {
		attr.errcount++
	}
}

func (pool *ChanConnPool) decAddrUnSafeCount(addr string) {
	pool.alocker.Lock()
	defer pool.alocker.Unlock()
	if attr, ok := pool.addrs[addr]; ok {
		if attr.errcount > 0 {
			attr.errcount--
		}
	}
}

func (pool *ChanConnPool) getAddrErrCount(addr string) int64 {
	pool.alocker.RLock()
	defer pool.alocker.RUnlock()
	if attr, ok := pool.addrs[addr]; ok {
		return attr.errcount
	}
	return int64(0)
}

func (pool *ChanConnPool) getCooldownTime(addr string) int64 {
	pool.alocker.RLock()
	defer pool.alocker.RUnlock()
	if attr, ok := pool.addrs[addr]; ok {
		return attr.cooldowntime
	}
	return int64(0)
}

func (pool *ChanConnPool) getAddrHealth(addr string) bool {
	pool.alocker.RLock()
	defer pool.alocker.RUnlock()
	if attr, ok := pool.addrs[addr]; ok {
		return attr.health
	}
	return true
}

func (pool *ChanConnPool) getHealthyRatio() float64 {
	pool.alocker.RLock()
	defer pool.alocker.RUnlock()
	healthcount := 0
	for _, attr := range pool.addrs {
		if attr.health {
			healthcount++
		}
	}
	return float64(healthcount) / float64(len(pool.addrs))
}

func (pool *ChanConnPool) resetAddr(addr string) {
	pool.alocker.Lock()
	defer pool.alocker.Unlock()
	if attr, ok := pool.addrs[addr]; ok {
		attr.cooldowntime = int64(0)
		attr.errcount = pool.healthyThreshold - int64(MaxRetryAfterCooldown)
		attr.health = true
	}
	al := []string{}
	for k, v := range pool.addrs {
		if v.health {
			al = append(al, k)
		}
	}
	sort.Strings(al)
	pool.al = al
}

func (pool *ChanConnPool) getRAddr(addrs map[string]*addrAttr) (string, error) {
	pool.alocker.Lock()
	defer pool.alocker.Unlock()
	host_len := len(addrs)
	if host_len == 0 {
		return "", errors.New("No address available.")
	} else {
		if len(pool.al) != 0 {
			return pool.al[pool.r.Intn(len(pool.al))], nil
		}
	}
	return "", errors.New("addrs is empty.")
}

//checker
//1.Whether unhealthy connections than the cooling cycle, if more than, the attribute the count left healthyThreshold - MaxRetryAfterCooldown health buy true cooldowntime = 0
//2.Whether health attribute the count of nodes exceeds the threshold, if more than threshold value will be false health, and set up the cooling time
//1.判断非健康连接是否超过了冷却周期，如果超过了，将属性count置healthyThreshold-MaxRetryAfterCooldown health置true cooldowntime=0
//2.判断健康节点中的属性count是否超过阈值，如果超过阈值将health置false，并设置冷却时间点
func (pool *ChanConnPool) check() {
	for {
		time.Sleep(pool.checkAddrCycle)
		for addr := range pool.getAllAddr() {
			if !pool.getAddrHealth(addr) {
				if time.Now().Unix()-pool.getCooldownTime(addr) > pool.maxCooldownTime {
					pool.resetAddr(addr)
				}
			} else {
				if pool.getAddrErrCount(addr) > pool.healthyThreshold {
					if pool.getHealthyRatio() > pool.minHealthyRatio && len(pool.al) > 1 {
						pool.setAddrdown(addr)
					} else {
						pool.resetAddr(addr)
					}
				}
			}
		}
	}
}

func (pool *ChanConnPool) newConn(addr string, timeout time.Duration) (interface{}, error) {
	for i := 0; i < 1+pool.retryTimes; i++ {
		client, err := pool.create(addr, timeout)
		if err != nil {
			continue
		} else {
			return client, nil
		}
	}
	return nil, errors.New("connect server fail.")
}

func (pool *ChanConnPool) getActive() int {
	active := pool.lenMapConn()
	for addr := range pool.getIdle() {
		active = active - pool.lenIdle(addr)
	}
	return active
}

func (pool *ChanConnPool) initIdle(size int) {
	idle := map[string]chan interface{}{}
	for addr := range pool.getAllAddr() {
		idle[addr] = make(chan interface{}, size)
	}
	pool.idle = idle
}

func (pool *ChanConnPool) getIdle() map[string]chan interface{} {
	return pool.idle
}

func (pool *ChanConnPool) inIdle(addr string, c interface{}) {
	if pool.lenIdle(addr) >= pool.capIdle(addr) {
		return
	}
	if queue, ok := pool.getIdle()[addr]; ok {
		queue <- c
		pool.setMapConnState(c, true)
	}
}

func (pool *ChanConnPool) outIdle(addr string) (interface{}, error) {
	if queue, ok := pool.getIdle()[addr]; ok {
		select {
		case client := <-queue:
			pool.setMapConnState(client, false)
			return client, nil
		default:
			cli, err := pool.newConn(addr, pool.connSvrTimeout)
			if err != nil {
				return nil, err
			}
			pool.setMapConnAddr(cli, addr, true)
			return cli, err
		}
	}
	return nil, errors.New("addr illegal.")
}

func (pool *ChanConnPool) lenIdle(addr string) int {
	if queue, ok := pool.getIdle()[addr]; ok {
		l := len(queue)
		return l
	}
	return 0
}

func (pool *ChanConnPool) capIdle(addr string) int {
	if queue, ok := pool.getIdle()[addr]; ok {
		c := cap(queue)
		return c
	}
	return 0
}

func (pool *ChanConnPool) reachLife(c interface{}) (reach bool) {
	life := pool.getConnLife(c)
	if pool.connLife == 0 {
		reach = false
	} else {
		reach = (time.Now().Unix() - life) > pool.connLife
	}
	return
}

func (pool *ChanConnPool) getAddrByConn(c interface{}) (string, bool) {
	addr, _, _, ok := pool.getMapConn(c)
	return addr, ok
}

func (pool *ChanConnPool) Get() (interface{}, error) {
	if addr, err := pool.getRAddr(pool.getAllAddr()); err != nil {
		return nil, err
	} else {
		cli, err := pool.outIdle(addr)
		if err != nil {
			pool.incAddrUnSafeCount(addr)
			return nil, err
		} else {
			return cli, nil
		}
	}
}

func (pool *ChanConnPool) Put(cli interface{}, safe bool) {
	if addr, _, borrow, ok := pool.getMapConn(cli); ok {
		if borrow {
			if pool.tryIncAddrConnCount(addr) {
				pool.setMapConnAddr(cli, addr, false)
			} else {
				pool.down(cli)
				pool.deleteMapConn(cli)
				return
			}
		}
		if safe {
			pool.decAddrUnSafeCount(addr)
		} else {
			pool.incAddrUnSafeCount(addr)
		}
		if !pool.isOpen(cli) || !safe || pool.reachLife(cli) {
			pool.down(cli)
			pool.deleteMapConn(cli)
			pool.decAddrConnCount(addr)
		} else {
			pool.inIdle(addr, cli)
		}
	}
	return
}

func (pool *ChanConnPool) GetHealthy() map[string]bool {
	healthy := map[string]bool{}
	for k, v := range pool.getAllAddr() {
		healthy[k] = v.health
	}
	return healthy
}

func (pool *ChanConnPool) GetUnhealthyNodeCount() (int, []string) {
	retCnt := 0
	retIp := []string{}

	for k, v := range pool.getAllAddr() {
		if !v.health {
			retCnt++
			retIp = append(retIp, k)
		}
	}

	return retCnt, retIp
}

func (pool *ChanConnPool) GetConnCount() map[string]int {
	res := map[string]int{}
	for addr := range pool.getAllAddr() {
		res[addr] = pool.getAddrConnCount(addr)
	}
	return res
}

func (pool *ChanConnPool) GetConnAllCount() int {
	ret := 0
	for addr := range pool.getAllAddr() {
		ret += pool.getAddrConnCount(addr)
	}
	return ret
}
