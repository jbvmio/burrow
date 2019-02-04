package burrow

import (
	"strconv"
	"time"

	"github.com/tidwall/gjson"
)

// Partition struct def
type Partition struct {
	Cluster      string    `json:"cluster"`
	Group        string    `json:"group"`
	CGStatus     string    `json:"cgstatus"`
	CGStatusCode int64     `json:"cgstatus_code"`
	Partitions   int64     `json:"partitions"`
	CGTotalLag   int64     `json:"cg_totallag"`
	Partition    int64     `json:"partition"`
	Topic        string    `json:"topic"`
	PStatus      string    `json:"pstatus"`
	PStatusCode  int64     `json:"pstatus_code"`
	StartOffset  int64     `json:"start_offset"`
	EndOffset    int64     `json:"end_offset"`
	HostNode     string    `json:"hostnode"`
	ClientID     string    `json:"client_id"`
	CurrentLag   int64     `json:"current_lag"`
	Complete     int64     `json:"complete"`
	TopicLag     int64     `json:"topic_totallag"`
	Timestamp    time.Time `json:"timestamp"`
}

// TopicConsumers struct def
type TopicConsumers struct {
	Cluster          string
	TopicConsumerMap map[string][]string
}

// TopicOffsets struct def
type TopicOffsets struct {
	TopicOffsets map[string][]int64
}

func getPartitions(r gjson.Result, parts *[]Partition) {

	cluster := r.Get("cluster").String()
	group := r.Get("group").String()
	cgstatus := r.Get("status").String()
	partitions := r.Get("partition_count").Int()
	totalLag := r.Get("totallag").Int()
	var i int64
	for i = 0; i <= partitions-1; i++ {
		var tl int64
		b := Partition{}
		b.Cluster = cluster
		b.Group = group
		b.CGStatus = cgstatus
		switch cstat := cgstatus; cstat {
		case "NOTFOUND":
			b.CGStatusCode = 0
		case "OK":
			b.CGStatusCode = 1
		case "WARN":
			b.CGStatusCode = 2
		case "ERR":
			b.CGStatusCode = 3
		case "STOP":
			b.CGStatusCode = 4
		case "STALL":
			b.CGStatusCode = 5
		}
		b.Partitions = partitions
		b.CGTotalLag = totalLag
		b.Partition = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".partition")).Int()
		b.Topic = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".topic")).String()
		b.PStatus = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".status")).String()
		b.HostNode = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".host")).String()
		b.ClientID = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".client_id")).String()
		switch pstat := b.PStatus; pstat {
		case "NOTFOUND":
			b.PStatusCode = 0
		case "OK":
			b.PStatusCode = 1
		case "WARN":
			b.PStatusCode = 2
		case "ERR":
			b.PStatusCode = 3
		case "STOP":
			b.PStatusCode = 4
		case "STALL":
			b.PStatusCode = 5
		}
		b.StartOffset = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".start.offset")).Int()
		b.EndOffset = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".end.offset")).Int()
		b.CurrentLag = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".current_lag")).Int()
		b.Complete = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".complete")).Int()
		if tl == 0 {
			x := r.Get(string("partitions.#[topic==" + b.Topic + "]#"))
			x.ForEach(func(key, value gjson.Result) bool {
				tl += value.Get("current_lag").Int()
				return true
			})
		}
		b.TopicLag = tl
		b.Timestamp = time.Time.UTC(time.Now())
		*parts = append(*parts, b)
	}

}

func returnPartitions(r gjson.Result) []Partition {
	//partitions := r.Get("partition_count").Int()
	var parts []Partition
	//parts := make([]Partition, 0, (partitions - 1))

	cluster := r.Get("cluster").String()
	group := r.Get("group").String()
	cgstatus := r.Get("status").String()
	partitions := r.Get("partition_count").Int()
	totalLag := r.Get("totallag").Int()
	var i int64
	for i = 0; i <= partitions-1; i++ {
		var tl int64
		b := Partition{}
		b.Cluster = cluster
		b.Group = group
		b.CGStatus = cgstatus
		switch cstat := cgstatus; cstat {
		case "NOTFOUND":
			b.CGStatusCode = 0
		case "OK":
			b.CGStatusCode = 1
		case "WARN":
			b.CGStatusCode = 2
		case "ERR":
			b.CGStatusCode = 3
		case "STOP":
			b.CGStatusCode = 4
		case "STALL":
			b.CGStatusCode = 5
		}
		b.Partitions = partitions
		b.CGTotalLag = totalLag
		b.Partition = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".partition")).Int()
		b.Topic = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".topic")).String()
		b.PStatus = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".status")).String()
		b.HostNode = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".host")).String()
		b.ClientID = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".client_id")).String()
		switch pstat := b.PStatus; pstat {
		case "NOTFOUND":
			b.PStatusCode = 0
		case "OK":
			b.PStatusCode = 1
		case "WARN":
			b.PStatusCode = 2
		case "ERR":
			b.PStatusCode = 3
		case "STOP":
			b.PStatusCode = 4
		case "STALL":
			b.PStatusCode = 5
		}
		b.StartOffset = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".start.offset")).Int()
		b.EndOffset = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".end.offset")).Int()
		b.CurrentLag = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".current_lag")).Int()
		b.Complete = r.Get(string("partitions." + strconv.FormatInt(i, 10) + ".complete")).Int()
		if tl == 0 {
			x := r.Get(string("partitions.#[topic==" + b.Topic + "]#"))
			x.ForEach(func(key, value gjson.Result) bool {
				tl += value.Get("current_lag").Int()
				return true
			})
		}
		b.TopicLag = tl
		b.Timestamp = time.Time.UTC(time.Now())
		parts = append(parts, b)
	}
	return parts
}
