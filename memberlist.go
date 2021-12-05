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
    items      = map[string]string{}
    broadcasts *memberlist.TransmitLimitedQueue
)

type update struct {
    Action string // set, del
    Data   map[string]string
}

func init() {
    flag.Parse()
}

type broadcast struct {
    msg    []byte
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
        var updates []*update
        if err := json.Unmarshal(b[1:], &updates); err != nil {
            return
        }
        mtx.Lock()
        for _, u := range updates {
            for k, v := range u.Data {
                switch u.Action {
                case "set":
                    items[k] = v
                case "del":
                    delete(items, k)
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
    m := items
    mtx.RUnlock()
    b, _ := json.Marshal(m)
    return b
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
    if len(buf) == 0 {
        return
    }

    if join {
        fmt.Println("join merge remote state")
    } else {
        fmt.Println("push/pull merge remote state")
    }

    var m map[string]string
    if err := json.Unmarshal(buf, &m); err != nil {
        return
    }
    mtx.Lock()
    for k, v := range m {
        items[k] = v
    }
    mtx.Unlock()
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

func setHandler(w http.ResponseWriter, r *http.Request) {
    _ = r.ParseForm()
    key := r.Form.Get("key")
    val := r.Form.Get("val")
    mtx.Lock()
    items[key] = val
    mtx.Unlock()

    b, err := json.Marshal([]*update{
        {
            Action: "set",
            Data: map[string]string{
                key: val,
            },
        },
    })

    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    broadcasts.QueueBroadcast(&broadcast{
        msg:    append([]byte("d"), b...),
    })
}

func delHandler(w http.ResponseWriter, r *http.Request) {
    _ = r.ParseForm()
    key := r.Form.Get("key")
    mtx.Lock()
    delete(items, key)
    mtx.Unlock()

    b, err := json.Marshal([]*update{{
        Action: "del",
        Data: map[string]string{
            key: "",
        },
    }})

    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    broadcasts.QueueBroadcast(&broadcast{
        msg:    append([]byte("d"), b...),
    })
}

func getHandler(w http.ResponseWriter, r *http.Request) {
    _ = r.ParseForm()
    key := r.Form.Get("key")
    mtx.RLock()
    val := items[key]
    mtx.RUnlock()
    _, _ = w.Write([]byte(val))
}

func start() error {
    hostname, _ := os.Hostname()
    c := memberlist.DefaultLocalConfig()
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
        return err
    }
    if len(*members) > 0 {
        parts := strings.Split(*members, ",")
        _, err := m.Join(parts)
        if err != nil {
            return err
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
    return nil
}

func main() {
    if err := start(); err != nil {
        fmt.Println(err)
    }

    http.HandleFunc("/set", setHandler)
    http.HandleFunc("/del", delHandler)
    http.HandleFunc("/get", getHandler)
    if err := http.ListenAndServe(":4396", nil); err != nil {
        fmt.Println(err)
    }
}