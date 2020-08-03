package configmap

import (
	"strings"

	"github.com/micro/go-micro/v2/config/encoder"
	"github.com/micro/go-micro/v2/config/encoder/json"
	"github.com/micro/go-micro/v2/config/encoder/yaml"
	"github.com/micro/go-micro/v2/logger"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	yamlEncoder = yaml.NewEncoder()
	jsonEncoder = json.NewEncoder()
)

func getClient(configPath string) (*kubernetes.Clientset, error) {
	var config *rest.Config
	var err error

	if configPath == "" {
		config, err = rest.InClusterConfig()
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", configPath)
	}

	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

func parse(enc encoder.Encoder, m map[string]interface{}, k, v, s string) {
	d := make(map[string]interface{})
	err := enc.Decode([]byte(v), &d)
	if err != nil {
		logger.Errorf("Failed to parse config data %s, error: %s", v, err)
	}
	m[strings.TrimSuffix(k, s)] = d
}

func makeMap(kv map[string]string) map[string]interface{} {

	data := make(map[string]interface{})

	for k, v := range kv {
		if strings.HasSuffix(k, ".yaml") {
			parse(yamlEncoder, data, k, v, ".yaml")
		} else if strings.HasSuffix(k, ".yml") {
			parse(yamlEncoder, data, k, v, ".yml")
		} else if strings.HasSuffix(k, ".json") {
			parse(jsonEncoder, data, k, v, ".json")
		} else {
			data[k] = make(map[string]interface{})

			vals := strings.Split(v, "\n")

			mp := make(map[string]interface{})
			for _, h := range vals {
				m, n := split(h, "=")
				mp[m] = n
			}

			data[k] = mp
		}
	}

	return data
}

func split(s string, sp string) (k string, v string) {
	i := strings.Index(s, sp)
	if i == -1 {
		return s, ""
	}
	return s[:i], s[i+1:]
}
