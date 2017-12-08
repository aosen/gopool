# gopool
* By Golang realize distributed common connection pool.
* Golang分布式的连接池，协程池，内含redis client连接池实现

* go get github.com/aosen/gopool

## example
```
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
```
