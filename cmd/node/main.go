package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

type Node struct {
	addr  string
	peers []string
	db    *sql.DB
}

func hkey(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func (n *Node) isOwner(key string) bool {
	// trivial: lowest hash between me and peers "owns" the key
	me := hkey(n.addr + key)
	min := me
	for _, p := range n.peers {
		if h := hkey(p + key); h < min {
			return false
		}
	}
	return true
}

func (n *Node) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct{ SQL string `json:"sql"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if !n.isOwner(req.SQL) && len(n.peers) > 0 {
		// forward to first peer for demo
		http.Post("http://localhost"+n.peers[0]+"/query", "application/json", strings.NewReader(string(must(json.Marshal(req)))))
		w.WriteHeader(202)
		w.Write([]byte("forwarded"))
		return
	}
	rows, err := n.db.Query(req.SQL)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals { ptrs[i] = &vals[i] }
		if err := rows.Scan(ptrs...); err != nil { http.Error(w, err.Error(), 500); return }
		row := map[string]any{}
		for i, c := range cols { row[c] = vals[i] }
		out = append(out, row)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func must(b []byte, _ error) []byte { return b }

func (n *Node) start() {
	http.HandleFunc("/query", n.handleQuery)
	log.Printf("listening %s peers=%v", n.addr, n.peers)
	http.ListenAndServe(n.addr, nil)
}

func main() {
	addr := ":7001"
	peers := []string{}
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--addr" { addr = os.Args[i+1]; i++
		} else if os.Args[i] == "--peers" { peers = strings.Split(os.Args[i+1], ","); i++ }
	}
	db, _ := sql.Open("sqlite", "file:data.db?cache=shared&mode=memory")
	db.Exec("create table if not exists kv(k text, v text)")
	db.Exec("insert into kv values('a','1'),('b','2')")
	n := &Node{addr: addr, peers: peers, db: db}
	n.start()
}
