package burrow

import "github.com/tidwall/gjson"

const basePath = "/v3/kafka"

// Client here
type Client struct {
	clusterURLs    []string
	burrowClusters map[string]string
	targetCluster  map[string]string
}

// NewBurrowClient returns a new Client. Set all to true to use all burrow clusters found,
// otherwise, subsequent commands it will return after the first if successful.
func NewBurrowClient(urls []string) (*Client, error) {
	var (
		addr   []string
		client Client
		err    error
	)
	for _, u := range urls {
		addr = append(addr, string(u+basePath))
	}
	client.burrowClusters, err = getBurrowClusters(addr)
	if err != nil {
		return &client, err
	}
	client.targetCluster = client.burrowClusters
	client.clusterURLs = addr
	return &client, err
}

// TargetCluster sets a specific burrow cluster instead of using all discovered clusters (default)
// If the cluster doesn't exist, no changes are made
func (bc *Client) TargetCluster(cluster string) {
	url := bc.burrowClusters[cluster]
	if url == "" {
		return
	}
	target := make(map[string]string, 1)
	target[cluster] = url
	bc.targetCluster = target
}

// TargetAll resets back to using all found burrow clusters
func (bc *Client) TargetAll() {
	bc.targetCluster = bc.burrowClusters
}

// GetClusterURLs here
func (bc *Client) GetClusterURLs() []string {
	return bc.clusterURLs
}

func getBurrowClusters(urls []string) (map[string]string, error) {
	var (
		clusters = make(map[string]string)
		errd     error
	)
	for _, u := range urls {
		clu, err := curlIt(u)
		if err == nil {
			c := gjson.GetBytes(clu, "clusters").Array()
			for _, i := range c {
				clusters[i.String()] = u
			}
		} else {
			errd = err
		}
	}
	return clusters, errd
}
