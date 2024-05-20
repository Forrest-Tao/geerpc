package registry

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

/*
	健康检查
	节点运行 http 服务
	发送 http请求，判断是否为可达节点
*/

// GeeRegistry is a simple register center ,provide following functions
// add a server and receive heatbeat to keep it alive
// returns all alive servers and delete dead servers sync simultaneously
type GeeRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_geerpc_/registry"
	defaultTimeout = time.Minute * 5
)

func New(timeout time.Duration) *GeeRegistry {
	return &GeeRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultGeeRegister = New(defaultTimeout)

// putServer
func (r *GeeRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.servers[addr]
	if item == nil {
		r.servers[addr] = &ServerItem{
			Addr:  addr,
			start: time.Now(),
		}
	} else {
		item.start = time.Now()
	}
}

func (r *GeeRegistry) aliveServes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	return alive
}

func (r *GeeRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// get alive servers
		w.Header().Set("X-Geerpc-Servers", strings.Join(r.aliveServes(), ","))
	case "POST":
		//judge whether the server is alive
		addr := req.Header.Get("X-Geerpc-Servers")
		if addr == "" {
			//500
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		//405
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *GeeRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path: ", registryPath)
}

func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}

func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeatBeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			select {
			case <-t.C:
				err = sendHeatBeat(registry, addr)
			}
		}
	}()
}

func sendHeatBeat(registry, addr string) (err error) {
	log.Println(addr, "send heart beat to registry ")
	httpClient := http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Servers", addr)
	if _, err = httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return
}
