package v1alpha1

import (
	"k8s.io/kubernetes/pkg/api/rest"
	"k8s.io/kubernetes/pkg/registry/generic/registry"
)

type serviceInstanceStorage struct {
}

func NewServiceInstanceStorage() rest.Storage {
	store := &genericregistry.Store{
		NewFunc: func() runtime.Object {
			return &Broker{}
		},
		// NewListFunc returns an object capable of storing results of an etcd list.
		NewListFunc: func() runtime.Object {
			return &BrokerList{}
		},
		// Retrieve the name field of the resource.
		ObjectNameFunc: func(obj runtime.Object) (string, error) {
			broker, ok := obj.(*Broker)
			if !ok {
				return "", errNotABroker
			}
			return broker.Name, nil
		},
		// Used to match objects based on labels/fields for list.
		PredicateFunc: matcher,
		// QualifiedResource should always be plural
		QualifiedResource: api.Resource("testtypes"),

		CreateStrategy: strategy,
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: getAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		panic(err) // TODO: Propagate error up
	}
	return &store
}
