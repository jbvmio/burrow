package burrow

import (
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func curlIt(url string) ([]byte, error) {
	var (
		body []byte
		err  error
	)
	resp, err := http.Get(url)
	if err != nil {
		return body, err
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return body, err
	}
	return body, nil
}

func dnsResolve(ip string) string {
	hn, err := net.LookupAddr(ip)
	if err == nil {
		return strings.Trim(hn[0], ".")
	}
	return ip
}

func parseHost(s string) string {
	return strings.Split(s, ".")[0]
}

func divMod(numerator, denominator int) (quotient, remainder int) {
	quotient = numerator / denominator // integer division, decimals are truncated
	remainder = numerator % denominator
	return
}

func stringsToMaps(someSlice []string, chunks int) map[string][]string {
	q, r := divMod(len(someSlice), chunks)
	myMap := make(map[string][]string)
	label := "chunk"
	var perChunk = q
	var lastChunk = q + r
	var progress int
	var count int

	for progress < len(someSlice)-lastChunk {
		count++
		k := string(label + strconv.Itoa(count))
		v := progress + perChunk
		myMap[k] = someSlice[progress:v]
		progress = v
	}
	myMap[string(label+strconv.Itoa(chunks))] = someSlice[progress:]
	return myMap
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func fileExists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

func writeFile(data []byte, filePath string) error {
	return ioutil.WriteFile(filePath, data, 0644)
}

func readFile(filePath string) ([]byte, error) {
	return ioutil.ReadFile(filePath)
}

func deleteFile(filePath string) error {
	return os.Remove(filePath)
}
