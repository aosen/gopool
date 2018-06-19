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
	"errors"
	"strconv"
	"time"

	"github.com/aosen/gopool"
	"github.com/garyburd/redigo/redis"
	"github.com/juju/ratelimit"
)

const (
	DEFAULT_ADDR         = "127.0.0.1:6379"
	DEFAULT_SIZE         = 100
	DEFAULT_CONNTIMEOUT  = 100
	DEFAULT_READTIMEOUT  = 50
	DEFAULT_WRITETIMEOUT = 50
	DEFAULT_MAXQPS       = 100000
)

type Req struct {
	Addrs        []string
	Size         int
	ConnTimeOut  int64
	ReadTimeOut  int64
	WriteTimeOut int64
	MaxQps       int64
}

func (req *Req) GetAddrs() []string {
	if len(req.Addrs) == 0 {
		return []string{DEFAULT_ADDR}
	}
	return req.Addrs
}

func (req *Req) GetSize() int {
	if req.Size <= 0 {
		return DEFAULT_SIZE
	}
	return req.Size
}

func (req *Req) GetConnTimeOut() int64 {
	if req.ConnTimeOut <= int64(0) {
		return DEFAULT_CONNTIMEOUT
	}
	return req.ConnTimeOut
}

func (req *Req) GetReadTimeOut() int64 {
	if req.ReadTimeOut <= int64(0) {
		return DEFAULT_READTIMEOUT
	}
	return req.ReadTimeOut
}

func (req *Req) GetWriteTimeOut() int64 {
	if req.WriteTimeOut <= int64(0) {
		return DEFAULT_WRITETIMEOUT
	}
	return req.WriteTimeOut
}

func (req *Req) GetMaxQps() int64 {
	if req.MaxQps <= int64(0) {
		return DEFAULT_MAXQPS
	}
	return req.MaxQps
}

type RedisCli struct {
	pool         *gopool.ChanConnPool
	conntimeout  time.Duration
	readtimeout  time.Duration
	writetimeout time.Duration
	maxqps       int64
	bucket       *ratelimit.Bucket
}

func NewRedisCli(req *Req) (rc *RedisCli, err error) {
	if req == nil {
		err = errors.New("req is nil")
	}
	rc = &RedisCli{
		conntimeout:  time.Duration(req.GetConnTimeOut()) * time.Millisecond,
		readtimeout:  time.Duration(req.GetReadTimeOut()) * time.Millisecond,
		writetimeout: time.Duration(req.GetWriteTimeOut()) * time.Millisecond,
		maxqps:       req.GetMaxQps(),
		bucket:       ratelimit.NewBucket(time.Second/time.Duration(req.GetMaxQps()), req.GetMaxQps()),
	}
	create := func(addr string, timeout time.Duration) (cli interface{}, err error) {
		cli, err = redis.DialTimeout("tcp", addr, timeout, rc.readtimeout, rc.writetimeout)
		return
	}
	isOpen := func(c interface{}) bool {
		if c != nil {
			return true
		}
		return false
	}
	down := func(c interface{}) {
		if cli, ok := c.(redis.Conn); ok {
			cli.Close()
		}
	}
	rc.pool, err = gopool.NewChanConnPool(&gopool.ConnPoolReq{
		Addrs:          req.GetAddrs(),
		ConnSvrTimeOut: rc.conntimeout,
		Size:           req.GetSize(),
		Create:         create,
		IsOpen:         isOpen,
		Down:           down,
	})
	return
}

func (rc *RedisCli) get() (cli redis.Conn, err error) {
	var (
		c  interface{}
		ok bool
	)
	if rc.bucket.TakeAvailable(1) < 1 {
		err = errors.New("rate limit")
		return
	}
	c, err = rc.pool.Get()
	if err != nil {
		return
	}
	cli, ok = c.(redis.Conn)
	if !ok {
		err = errors.New("cli is nil.")
	}
	return
}

func (rc *RedisCli) put(cli redis.Conn, err *error) {
	safe := false
	if *err == nil || *err == redis.ErrNil {
		safe = true
	}
	rc.pool.Put(cli, safe)
}

func (rc *RedisCli) GetHealthy() map[string]bool {
	return rc.pool.GetHealthy()
}

func (rc *RedisCli) GetUnhealthyNodeCount() (int, []string) {
	return rc.pool.GetUnhealthyNodeCount()
}

func (rc *RedisCli) GetConnCount() map[string]int {
	return rc.pool.GetConnCount()
}

func (rc *RedisCli) GetConnAllCount() int {
	return rc.pool.GetConnAllCount()
}

func (rc *RedisCli) FlushAll() (err error) {
	var (
		cli redis.Conn
	)
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.String(cli.Do("FLUSHALL"))
	return
}

func (rc *RedisCli) Exists(key string) (ex bool, err error) {
	var (
		cli redis.Conn
		ret int
	)
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	ret, err = redis.Int(cli.Do("EXISTS", key))
	if ret == 1 {
		ex = true
		return
	}
	return
}

func (rc *RedisCli) Expire(key string, expiresecond int) (err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.Int(cli.Do("EXPIRE", key, expiresecond))
	return
}

func (rc *RedisCli) TTL(key string) (reply int, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.Int((cli.Do("TTL", key)))
	return
}

func (rc *RedisCli) Set(args ...interface{}) (err error) {
	var (
		cli redis.Conn
	)
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.String((cli.Do("SET", args...)))
	return
}

func (rc *RedisCli) Get(key string) (reply string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.String(cli.Do("GET", key))
	return
}

func (rc *RedisCli) MGet(keys ...interface{}) (replys []string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	replys, err = redis.Strings(cli.Do("MGET", keys...))
	return
}

func (rc *RedisCli) Setnx(key, value string) (err error) {
	var (
		cli redis.Conn
		ret int
	)
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	ret, err = redis.Int((cli.Do("SETNX", key, value)))
	if ret == 0 {
		err = errors.New("exist.")
	}
	return
}

func (rc *RedisCli) Del(key string) (err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.Int(cli.Do("DEL", key))
	return
}

func (rc *RedisCli) Incr(key string) (reply int, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.Int((cli.Do("INCR", key)))
	return
}

func (rc *RedisCli) IncrByfloat(key string, value float64) (reply string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.String(cli.Do("INCRBYFLOAT", key, value))
	return
}

func (rc *RedisCli) IncrBy(key string, value int) (reply int, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.Int(cli.Do("INCRBY", key, value))
	return
}

func (rc *RedisCli) SAdd(key string, args ...interface{}) (err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.Int((cli.Do("SADD", append([]interface{}{interface{}(key)}, args...)...)))
	return
}

func (rc *RedisCli) SRem(key string, args ...interface{}) (err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.Int((cli.Do("SREM", append([]interface{}{interface{}(key)}, args...)...)))
	return
}

func (rc *RedisCli) SPop(key string) (reply string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.String(cli.Do("SPOP", key))
	return
}

func (rc *RedisCli) SCard(key string) (reply int, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.Int(cli.Do("SCARD", key))
	return
}

func (rc *RedisCli) SMembers(key string) (reply []string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.Strings(cli.Do("SMEMBERS", key))
	return
}

func (rc *RedisCli) SUnionstore(dkey string, args ...interface{}) (count int64, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	count, err = redis.Int64((cli.Do("SUNIONSTORE", append([]interface{}{interface{}(dkey)}, args...)...)))
	return
}

//ZADD key score member [[score member] [score member] ...]
func (rc *RedisCli) ZAdd(key string, ttl int64, args ...interface{}) (err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.Int((cli.Do("ZADD", append([]interface{}{interface{}(key)}, args...)...)))

	//默认过期时间为86400
	err = rc.Expire(key, int(ttl))
	return
}

func (rc *RedisCli) ZRange(key string, start, stop int) (reply []string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.Strings(cli.Do("ZRANGE", key, start, stop))
	return
}

func (rc *RedisCli) ZRangeWithScores(key string, start, stop int) (reply map[string]string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.StringMap(cli.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	return
}

func (rc *RedisCli) ZRangeByScore(key string, min, max int, minopen, maxopen bool) (reply []string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	minstr := strconv.FormatInt(int64(min), 10)
	maxstr := strconv.FormatInt(int64(max), 10)
	if minopen {
		minstr = "(" + strconv.FormatInt(int64(min), 10)
	}
	if maxopen {
		maxstr = "(" + strconv.FormatInt(int64(max), 10)
	}
	if 0 == min {
		minstr = "-inf"
	}
	if -1 == max {
		maxstr = "+inf"
	}
	reply, err = redis.Strings(cli.Do("ZRANGEBYSCORE", key, minstr, maxstr))
	return
}

func (rc *RedisCli) ZRangeByScoreWithScores(key string, min, max int, minopen, maxopen bool) (reply map[string]string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	minstr := strconv.FormatInt(int64(min), 10)
	maxstr := strconv.FormatInt(int64(max), 10)
	if minopen {
		minstr = "(" + strconv.FormatInt(int64(min), 10)
	}
	if maxopen {
		maxstr = "(" + strconv.FormatInt(int64(max), 10)
	}
	reply, err = redis.StringMap(cli.Do("ZRANGEBYSCORE", key, minstr, maxstr, "WITHSCORES"))
	return
}

func (rc *RedisCli) ZRem(keys ...interface{}) (reply int64, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.Int64(cli.Do("ZREM", keys...))
	return
}

func (rc *RedisCli) ZRemRangeByScore(key string, min, max int64) (reply int64, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	minstr := strconv.FormatInt(min, 10)
	maxstr := strconv.FormatInt(max, 10)
	if 0 == min {
		minstr = "-inf"
	}
	if -1 == max {
		maxstr = "+inf"
	}
	reply, err = redis.Int64(cli.Do("ZREMRANGEBYSCORE", key, minstr, maxstr))
	return
}

func (rc *RedisCli) ZCount(key string, min, max int64) (reply int64, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	minstr := strconv.FormatInt(min, 10)
	maxstr := strconv.FormatInt(max, 10)
	if 0 == min {
		minstr = "-inf"
	}
	if -1 == max {
		maxstr = "+inf"
	}
	reply, err = redis.Int64(cli.Do("ZCOUNT", key, minstr, maxstr))
	return
}

func (rc *RedisCli) HSet(key, subkey, value string) (err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	_, err = redis.Int(cli.Do("HSET", key, subkey, value))
	return
}

func (rc *RedisCli) HGetall(key string) (reply map[string]string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.StringMap(cli.Do("HGETALL", key))
	return
}

func (rc *RedisCli) HGet(key, subkey string) (reply string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	reply, err = redis.String(cli.Do("HGET", key, subkey))
	return
}

func (rc *RedisCli) HKeys(key string) (replys []string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	replys, err = redis.Strings(cli.Do("HKEYS", key))
	return
}

func (rc *RedisCli) HMSet(key string, values []interface{}) (err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	params := make([]interface{}, 0)
	params = append(params, key)
	params = append(params, values...)
	_, err = redis.String((cli.Do("HMSET", params...)))
	return
}

func (rc *RedisCli) HMGet(key string, fields []interface{}) (replys []string, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	params := make([]interface{}, 0)
	params = append(params, key)
	params = append(params, fields...)

	replys, err = redis.Strings((cli.Do("HMGET", params...)))
	return
}

func (rc *RedisCli) HDel(key string, fields ...interface{}) (reply int64, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	params := make([]interface{}, 0)
	params = append(params, key)
	params = append(params, fields...)
	reply, err = redis.Int64((cli.Do("HDEL", params...)))
	return
}

func (rc *RedisCli) ZScore(key, val string) (score int64, err error) {
	var cli redis.Conn
	cli, err = rc.get()
	if err != nil {
		return
	}
	defer rc.put(cli, &err)
	score, err = redis.Int64(cli.Do("ZSCORE", key, val))
	return
}
