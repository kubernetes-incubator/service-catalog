/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file was automatically generated by informer-gen

package v1alpha1

import (
	servicecatalog_v1alpha1 "github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/v1alpha1"
	clientset "github.com/kubernetes-incubator/service-catalog/pkg/client/clientset_generated/clientset"
	internalinterfaces "github.com/kubernetes-incubator/service-catalog/pkg/client/informers_generated/externalversions/internalinterfaces"
	v1alpha1 "github.com/kubernetes-incubator/service-catalog/pkg/client/listers_generated/servicecatalog/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
	time "time"
)

// ServiceInstanceInformer provides access to a shared informer and lister for
// ServiceInstances.
type ServiceInstanceInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1alpha1.ServiceInstanceLister
}

type serviceInstanceInformer struct {
	factory internalinterfaces.SharedInformerFactory
}

func newServiceInstanceInformer(client clientset.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	sharedIndexInformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				return client.ServicecatalogV1alpha1().ServiceInstances(v1.NamespaceAll).List(options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				return client.ServicecatalogV1alpha1().ServiceInstances(v1.NamespaceAll).Watch(options)
			},
		},
		&servicecatalog_v1alpha1.ServiceInstance{},
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	return sharedIndexInformer
}

func (f *serviceInstanceInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&servicecatalog_v1alpha1.ServiceInstance{}, newServiceInstanceInformer)
}

func (f *serviceInstanceInformer) Lister() v1alpha1.ServiceInstanceLister {
	return v1alpha1.NewServiceInstanceLister(f.Informer().GetIndexer())
}
