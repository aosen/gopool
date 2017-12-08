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

General distributed connection manager interface
通用分布式连接池管理接口
*/
package gopool

type Pooler interface {
	//Get connected
	//获取连接
	Get() (interface{}, error)
	//Recycling connection
	//回收连接
	Put(client interface{}, safe bool)
	//Access to the health status of the downstream nodes
	//获取下游节点的健康状态
	GetHealthy() map[string]bool
	//The current number of connections for the downstream nodes
	//获取各下游节点当前连接数
	GetConnCount() map[string]int
}
