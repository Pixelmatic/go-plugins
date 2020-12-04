package kubernetes

import (
	"encoding/json"
	"errors"
	"strings"

	log "github.com/micro/go-micro/v2/logger"
	"github.com/micro/go-micro/v2/registry"
	"github.com/micro/go-plugins/registry/kubernetes/v2/client"
	"github.com/micro/go-plugins/registry/kubernetes/v2/client/watch"
)

type k8sWatcher struct {
	registry *kregistry
	watcher  watch.Watch
	next     chan *registry.Result
}

func (k *k8sWatcher) parse(pod client.Pod, action string) *registry.Result {
	if pod.Metadata != nil {
		for ak, av := range pod.Metadata.Annotations {
			// check this annotation kv is a service notation
			if !strings.HasPrefix(ak, annotationServiceKeyPrefix) || av == nil {
				continue
			}
			// unmarshal service notation from annotation value
			res := new(registry.Result)
			res.Action = action
			err := json.Unmarshal([]byte(*av), &res.Service)
			if err != nil {
				continue
			}
			return res
		}
	}
	return nil
}

// handleEvent will taken an event from the k8s pods API and do the correct
// things with the result, based on the local cache.
func (k *k8sWatcher) handleEvent(event watch.Event) bool {
	var pod client.Pod
	if err := json.Unmarshal([]byte(event.Object), &pod); err != nil {
		log.Error("K8s Watcher: Couldnt unmarshal event object from pod")
		return false
	}

	log.Debugf("New k8s registry event %v %s %s", event.Type, pod.Metadata.Name, pod.Status.Phase)

	action := "delete"
	switch event.Type {
	case watch.Added, watch.Modified:
		if pod.Status.Phase == podRunning {
			action = "create"
		}
	case watch.Deleted:
	default:
		log.Errorf("Unknown k8s watch event type %v, event.Type")
		return false
	}
	res := k.parse(pod, action)
	if res != nil {
		k.next <- res
	}
	return true
}

// Next will block until a new result comes in
func (k *k8sWatcher) Next() (*registry.Result, error) {
	r, ok := <-k.next
	if !ok {
		return nil, errors.New("result chan closed")
	}
	return r, nil
}

// Stop will cancel any requests, and close channels
func (k *k8sWatcher) Stop() {
	k.watcher.Stop()

	select {
	case <-k.next:
	default:
		close(k.next)
	}
}

func newWatcher(kr *kregistry, opts ...registry.WatchOption) (registry.Watcher, error) {
	var wo registry.WatchOptions
	for _, o := range opts {
		o(&wo)
	}

	selector := podSelector
	if len(wo.Service) > 0 {
		selector = map[string]string{
			svcSelectorPrefix + serviceName(wo.Service): svcSelectorValue,
		}
	}

	// Create watch request
	watcher, err := kr.client.WatchPods(selector)
	if err != nil {
		return nil, err
	}

	k := &k8sWatcher{
		registry: kr,
		watcher:  watcher,
		next:     make(chan *registry.Result, 10),
	}

	// range over watch request changes, and invoke
	// the update event
	go func() {
		for event := range watcher.ResultChan() {
			if !k.handleEvent(event) {
				break
			}
		}
		k.Stop()
	}()

	return k, nil
}
