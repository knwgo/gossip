package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
)

var (
	mtx        sync.RWMutex
	members    = flag.String("members", "", "comma separated list of members")
	local      = state{}
	broadcasts memberlist.TransmitLimitedQueue
)

type state map[string]*entry

type entry struct {
	Key, Val string
	Deleted  bool
}

type update struct {
	Action string
	Data   []state
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors: true,
	})
	log.SetLevel(log.DebugLevel)
	flag.Parse()
}

type broadcast struct {
	msg []byte
}

func (b *broadcast) Invalidates(_ memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	return
}

type delegate struct{}

func (d *delegate) NodeMeta(_ int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 || b[0] != 'd' {
		return
	}

	var u update
	if err := json.Unmarshal(b[1:], &u); err != nil {
		log.Error(err)
		return
	}
	mtx.Lock()
	switch u.Action {
	case "add":
		log.Info("receive add event")
		for i := range u.Data {
			for id := range u.Data[i] {
				if _, ok := local[id]; !ok {
					local[id] = u.Data[i][id]
					broadcasts.QueueBroadcast(&broadcast{
						msg: b,
					})
				}
			}
		}
	case "del":
		log.Info("receive del event")
		var changed bool
		for _, data := range u.Data {
			for id := range data {
				if _, ok := local[id]; ok {
					if local[id].Deleted {
						continue
					}
					local[id].Deleted = true
					changed = true
				}
			}
		}
		if changed {
			broadcasts.QueueBroadcast(&broadcast{
				msg: b,
			})
		}
	}
	mtx.Unlock()
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(_ bool) []byte {
	mtx.RLock()
	m := local
	mtx.RUnlock()
	b, _ := json.Marshal(m)
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	_ = join

	if len(buf) == 0 {
		return
	}

	remote := map[string]*entry{}
	if err := json.Unmarshal(buf, &remote); err != nil {
		log.Error(err)
		return
	}
	mtx.Lock()
	for id := range remote {
		if entry, ok := local[id]; !ok {
			local[id] = entry
		} else {
			if !local[id].Deleted && remote[id].Deleted {
				local[id].Deleted = true
			}
		}
	}
	mtx.Unlock()
}

type eventDelegate struct{}

func (ed *eventDelegate) NotifyJoin(node *memberlist.Node) {
	log.Infof("a node has joined: %s", node.String())
}

func (ed *eventDelegate) NotifyLeave(node *memberlist.Node) {
	log.Warningf("a node has left: %s", node.String())
}

func (ed *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	log.Warningf("a node was updated: %s", node.String())
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	key := r.Form.Get("key")
	val := r.Form.Get("val")
	mtx.Lock()
	add := &entry{
		Key:     key,
		Val:     val,
		Deleted: false,
	}
	id := uuid.NewUUID().String()
	local[id] = add
	mtx.Unlock()

	b, err := json.Marshal(update{
		Action: "add",
		Data: []state{{id: add}},
	})

	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	broadcasts.QueueBroadcast(&broadcast{
		msg: append([]byte("d"), b...),
	})
}

func delHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	key := r.Form.Get("key")
	var deleted []state
	mtx.Lock()
	for id := range local {
		if local[id].Key == key {
			local[id].Deleted = true
			deleted = append(deleted, state{id: nil})
		}
	}
	mtx.Unlock()

	b, err := json.Marshal(update{
		Action: "del",
		Data:   deleted,
	})

	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	broadcasts.QueueBroadcast(&broadcast{
		msg: append([]byte("d"), b...),
	})
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	key := r.Form.Get("key")
	var val []string
	mtx.RLock()
	for _, entry := range local {
		if entry.Key == key {
			if !entry.Deleted {
				val = append(val, entry.Val)
			}
		}
	}
	mtx.RUnlock()
	_, _ = w.Write([]byte(strings.Join(val, ",")))
}

type logOutput struct {}

func (l *logOutput) Write(p []byte) (n int, err error) {
	log.Debug(string(p))
	return len(p), nil
}

func start() (*memberlist.Memberlist, error) {
	hostname, _ := os.Hostname()
	c := memberlist.DefaultLANConfig()
	c.Events = &eventDelegate{}
	c.Delegate = &delegate{}
	c.BindPort = 33333
	c.AdvertiseAddr = os.Getenv("POD_IP")
	c.AdvertisePort = 33333
	c.Name = hostname + "-" + uuid.NewUUID().String()
	c.LogOutput = &logOutput{}
	m, err := memberlist.Create(c)
	if err != nil {
		return m, err
	}

	broadcasts = memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: 3,
	}

	if len(*members) > 0 {
		for {
			time.Sleep(time.Second * 3)
			parts := strings.Split(*members, ",")
			_, err := m.Join(parts)
			if err == nil {
				break
			}
		}
	}

	node := m.LocalNode()
	log.Debugf("local member %s:%d\n", node.Addr, node.Port)
	return m, nil
}

func main() {
	_, err := start()
	if err != nil {
		log.Error(err)
	}

	go gc()

	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/del", delHandler)
	http.HandleFunc("/get", getHandler)
	if err := http.ListenAndServe(":4396", nil); err != nil {
		log.Panic(err)
	}
}

func gc() {
	tick := time.NewTicker(time.Minute * 15)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			log.Info("start gc")
			mtx.Lock()
			l1 := len(local)
			for i := range local {
				if local[i].Deleted {
					delete(local, i)
				}
			}
			l2 := len(local)
			log.Debugf("complete gc, delete %d entry", l1 - l2)
			mtx.Unlock()
		}
	}
}
