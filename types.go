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

Instantiation of the connection pool configuration parameters
实例化连接池的配置参数
*/
package gopool

import "time"

const (
	//The default timeout connection services
	//默认连接服务的超时时间
	DefaultConnSvrTimeOut = "100ms"
	//Health check time period
	//健康检查的时间周期
	DefaultCheckAddrCycle = "3s"
	//The default connection service retries
	//默认连接服务的重试次数
	DefaultRetryTimes = 1
	//Connection pool size
	//连接池大小
	DefaultSize = 20
	//Default health threshold, the threshold value of the node to node, unhealthy and cooling
	//默认健康阈值，达到阈值将节点置为不健康节点，并冷却
	DefaultHealthyThreshold = 20
	//The default maximum cooling time, seconds
	//默认最大冷却时长 秒
	DefaultMaxCooldownTime = 120
	//Than the default minimum health, prevent all removed, If only one downstream, does not remove
	//默认最小健康比, 防止全部摘除, 如果只有一个下游，则不会摘除
	DefaultMinHealthyRatio = 0.8
	//The biggest life cycle, the default connection to life cycle will be recycled
	//默认连接的最大生命周期, 达到生命周期会被回收
	DefaultConnLife = 3600
)

//初始化连接池请求格式
type ConnPoolReq struct {
	//Downstream address list
	//下游地址列表
	Addrs []string
	//Connection timeout of service
	//连接服务的超时时间
	ConnSvrTimeOut time.Duration
	//Health check time period
	//健康检查的时间周期
	CheckAddrCycle time.Duration
	//Connected to the downstream service retries
	//连接下游服务的重试次数
	RetryTimes int
	//Maximum number of connections, connection pool size
	//最大连接数，连接池大小
	Size int
	//Threshold, the health to nodes threshold will be set to false unhealthy node
	//健康阈值，达到阈值将节点置为不健康节点 false
	HealthyThreshold int64
	//The maximum cooling time. seconds
	//最大冷却时长 秒
	MaxCooldownTime int64
	//Than the minimum health, prevent all removed
	//最小健康比, 防止全部摘除
	MinHealthyRatio float64
	//Connect the biggest life cycle
	//连接最大生命周期
	ConnLife int64
	//Creating a connection to generate the client method
	//创建连接生成客户端的方法
	Create func(addr string, timeout time.Duration) (interface{}, error)
	//To determine whether a connection closed
	//判断连接是否关闭
	IsOpen func(c interface{}) bool
	//Close the connection
	//关闭连接
	Down func(c interface{})
}

func (req *ConnPoolReq) GetConnSvrTimeOut() time.Duration {
	if int64(req.ConnSvrTimeOut) <= int64(0) {
		tm, _ := time.ParseDuration(DefaultConnSvrTimeOut)
		return tm
	}
	return req.ConnSvrTimeOut
}

func (req *ConnPoolReq) GetCheckAddrCycle() time.Duration {
	if int64(req.CheckAddrCycle) <= int64(0) {
		tm, _ := time.ParseDuration(DefaultCheckAddrCycle)
		return tm
	}
	return req.CheckAddrCycle
}

func (req *ConnPoolReq) GetRetryTimes() int {
	if req.RetryTimes <= 0 {
		return DefaultRetryTimes
	}
	return req.RetryTimes
}

func (req *ConnPoolReq) GetSize() int {
	if req.Size <= 0 {
		return DefaultSize
	}
	return req.Size
}

func (req *ConnPoolReq) GetHealthyThreshold() int64 {
	if req.HealthyThreshold <= 0 {
		return DefaultHealthyThreshold
	}
	return req.HealthyThreshold
}

func (req *ConnPoolReq) GetMaxCooldownTime() int64 {
	if req.MaxCooldownTime <= 0 {
		return DefaultMaxCooldownTime
	}
	return req.MaxCooldownTime
}

func (req *ConnPoolReq) GetMinHealthyRatio() float64 {
	if req.MinHealthyRatio <= 0 && req.MinHealthyRatio > 1 {
		return DefaultMinHealthyRatio
	}
	return req.MinHealthyRatio
}

func (req *ConnPoolReq) GetConnLife() int64 {
	if req.ConnLife <= 0 {
		req.ConnLife = DefaultConnLife
	}
	return req.ConnLife
}
