package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
)

var (
	mtx        sync.RWMutex
	members    = flag.String("members", "", "comma separated list of members")
	local      []entry
	broadcasts *memberlist.TransmitLimitedQueue
)


type entry struct {
	Id       string
	Key, Val string
	Expired  time.Time
}

type update struct {
	Action string // add, del
	Data   []entry
}

func init() {
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
	if len(b) == 0 {
		return
	}

	switch b[0] {
	case 'd': // data
		fmt.Println("receive user data")
		var updates []update
		if err := json.Unmarshal(b[1:], &updates); err != nil {
			fmt.Println(err)
			return
		}
		mtx.Lock()
		for _, u := range updates {
			for i := range u.Data {
				switch u.Action {
				case "add":
					fmt.Println("receive add event")
					if !contain(local, u.Data[i].Id) {
						local = append(local, u.Data[i])
						broadcasts.QueueBroadcast(&broadcast{
							msg: b,
						})
					}
				case "del":
					fmt.Println("receive del event")
					var changed bool
					if contain(local, u.Data[i].Id) {
						for i := range local {
							if local[i].Id == u.Data[i].Id {
								if !local[i].Expired.IsZero() && local[i].Expired.Before(time.Now()) {
									continue
								}
								local[i].Expired = u.Data[i].Expired
								changed = true
							}
						}
						if changed {
							broadcasts.QueueBroadcast(&broadcast{
								msg: b,
							})
						}
					}
				}
			}
		}
		mtx.Unlock()
	}
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
	if len(buf) == 0 {
		return
	}

	var remote []entry
	if err := json.Unmarshal(buf, &remote); err != nil {
		return
	}
	mtx.Lock()
	for i := range remote {
		if !contain(local, remote[i].Id) {
			local = append(local, remote[i])
		}
	}
	mtx.Unlock()
}

func contain(et []entry, s string) bool {
	for _, e := range et {
		if e.Id == s {
			return true
		}
	}
	return false
}

type eventDelegate struct{}

func (ed *eventDelegate) NotifyJoin(node *memberlist.Node) {
	fmt.Println("A node has joined: " + node.String())
}

func (ed *eventDelegate) NotifyLeave(node *memberlist.Node) {
	fmt.Println("A node has left: " + node.String())
}

func (ed *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	fmt.Println("A node was updated: " + node.String())
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	key := r.Form.Get("key")
	val := r.Form.Get("val")
	mtx.Lock()
	add := entry{
		Id:      uuid.NewUUID().String(),
		Key:     key,
		Val:     val,
		Expired: time.Time{},
	}
	local = append(local, add)
	mtx.Unlock()

	b, err := json.Marshal([]update{
		{
			Action: "add",
			Data:   []entry{add},
		},
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
	now := time.Now()
	var deleted []entry
	mtx.Lock()
	for i := range local {
		if local[i].Key == key {
			local[i].Expired = now
			deleted = append(deleted, local[i])
		}
	}
	mtx.Unlock()

	b, err := json.Marshal([]update{{
		Action: "del",
		Data:   deleted,
	}})

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
	for i := range local {
		if local[i].Key == key {
			expireTime := local[i].Expired
			if expireTime.IsZero() || expireTime.After(time.Now()) {
				val = append(val, local[i].Val)
			}
		}
	}
	mtx.RUnlock()
	_, _ = w.Write([]byte(strings.Join(val, ",")))
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
	c.GossipInterval = time.Millisecond * 200
	c.GossipNodes = 3
	m, err := memberlist.Create(c)
	if err != nil {
		return m, err
	}
	if len(*members) > 0 {
		parts := strings.Split(*members, ",")
		_, err := m.Join(parts)
		if err != nil {
			return m, err
		}
	}
	broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: 3,
	}
	node := m.LocalNode()
	fmt.Printf("Local member %s:%d\n", node.Addr, node.Port)
	return m, nil
}

func main() {
	_, err := start()
	if err != nil {
		fmt.Println(err)
	}
	go printMembersStatus()

	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/del", delHandler)
	http.HandleFunc("/get", getHandler)
	if err := http.ListenAndServe(":4396", nil); err != nil {
		fmt.Println(err)
	}
}

func printMembersStatus() {
	tick := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-tick.C:
			fmt.Println(strings.Repeat("#", 50))
			fmt.Println(local)
			fmt.Println(strings.Repeat("#", 50))
		}
	}
}
