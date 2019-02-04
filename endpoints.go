package burrow

import (
	"fmt"

	"github.com/tidwall/gjson"
)

// GetClusterList here
func (bc *Client) GetClusterList() map[string]string {
	return bc.burrowClusters
}

// GetConsumers here
func (bc *Client) GetConsumers(consumer ...string) ([]Partition, error) {
	var (
		partitions []Partition
		err        error
	)
	for _, x := range consumer {
		path := string("/consumer/" + x)
		for k, v := range bc.targetCluster {
			ep := string(v + "/" + k + path + "/lag")
			lag, err := curlIt(ep)
			if err == nil {
				c := gjson.GetBytes(lag, "status")
				getPartitions(c, &partitions)
			}
		}
	}
	return partitions, err
}

// GetConsumerPartitions here
func (bc *Client) GetConsumerPartitions(consumer ...string) ([]Partition, error) {
	var (
		partitions []Partition
		err        error
		errCount   int
	)
	var partChan = make(chan []Partition, 100)
	var errChan = make(chan error, 100)
	for _, x := range consumer {
		path := string("/consumer/" + x)
		for k, v := range bc.targetCluster {
			ep := string(v + "/" + k + path + "/lag")
			go func(ep string, partChan chan []Partition, errChan chan error) {
				lag, err := curlIt(ep)
				if err == nil {
					c := gjson.GetBytes(lag, "status")
					partChan <- returnPartitions(c)
				} else {
					errChan <- err
				}
			}(ep, partChan, errChan)
		}
	}
	for i := 0; i < len(consumer)*len(bc.targetCluster); i++ {
		select {
		case p := <-partChan:
			partitions = append(partitions, p...)
		case e := <-errChan:
			err = e
			errCount++
		}
	}
	if errCount > 1 {
		err = fmt.Errorf("there were %v error(s) encountered, last: %v", errCount, err)
	}
	return partitions, err
}

// remove this
func goGetConsumerParts(ep string, partChan chan []Partition, errChan chan error) {
	lag, err := curlIt(ep)
	if err == nil {
		c := gjson.GetBytes(lag, "status")
		partChan <- returnPartitions(c)
	} else {
		errChan <- err
	}
}

// GetTopicList here
func (bc *Client) GetTopicList() ([]string, error) {
	var (
		topics []string
		err    error
	)
	path := string("/topic")
	for k, v := range bc.targetCluster {
		ep := string(v + "/" + k + path)
		tops, err := curlIt(ep)
		if err == nil {
			c := gjson.GetBytes(tops, "topics").Array()
			for _, t := range c {
				topics = append(topics, t.String())
			}
		}
	}
	return topics, err
}

// GetConsumerList here
func (bc *Client) GetConsumerList() ([]string, error) {
	var (
		groups []string
		err    error
	)
	path := string("/consumer")
	for k, v := range bc.targetCluster {
		ep := string(v + "/" + k + path)
		cons, err := curlIt(ep)
		if err == nil {
			c := gjson.GetBytes(cons, "consumers").Array()
			for _, g := range c {
				groups = append(groups, g.String())
			}
		}
	}
	return groups, err
}

/*
// GetTopicDetail here
func (bc *Client) GetTopicDetail() TopicOffsets {

}
*/

// GetTopicConsumersList here
func (bc *Client) GetTopicConsumersList() ([]TopicConsumers, error) {
	var topicConsumers []TopicConsumers
	topicConsumerMap := make(map[string][]string)
	var err error
	origCluster := bc.targetCluster
	for k, v := range bc.targetCluster {
		topicConsumer := TopicConsumers{
			Cluster: k,
		}
		bc.TargetCluster(k)
		allTopics, err := bc.GetTopicList()
		if err != nil {
			return topicConsumers, err
		}
		for _, topic := range allTopics {
			var groups []string
			path := string("/topic/" + topic + "/consumers")
			ep := string(v + "/" + k + path)
			cons, err := curlIt(ep)
			if err == nil {
				c := gjson.GetBytes(cons, "consumers").Array()
				for _, g := range c {
					groups = append(groups, g.String())
				}
				topicConsumerMap[topic] = groups
			}
		}
		topicConsumer.TopicConsumerMap = topicConsumerMap
		topicConsumers = append(topicConsumers, topicConsumer)
	}
	bc.targetCluster = origCluster
	return topicConsumers, err
}

/*
	// All valid paths go here
	hc.router.GET("/v3/kafka", hc.handleClusterList)
	hc.router.GET("/v3/kafka/:cluster", hc.handleClusterDetail)
	hc.router.GET("/v3/kafka/:cluster/topic", hc.handleTopicList)
	hc.router.GET("/v3/kafka/:cluster/topic/:topic", hc.handleTopicDetail)
	hc.router.GET("/v3/kafka/:cluster/topic/:topic/consumers", hc.handleTopicConsumerList)
	hc.router.GET("/v3/kafka/:cluster/consumer", hc.handleConsumerList)
	hc.router.GET("/v3/kafka/:cluster/consumer/:consumer", hc.handleConsumerDetail)
	hc.router.GET("/v3/kafka/:cluster/consumer/:consumer/status", hc.handleConsumerStatus)
	hc.router.GET("/v3/kafka/:cluster/consumer/:consumer/lag", hc.handleConsumerStatusComplete)
*/
