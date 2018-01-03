/*
Copyright 2018 The Kubernetes Authors.

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

// This file was automatically generated by lister-gen

package v1beta1

import (
	v1beta1 "github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// ClusterServiceClassLister helps list ClusterServiceClasses.
type ClusterServiceClassLister interface {
	// List lists all ClusterServiceClasses in the indexer.
	List(selector labels.Selector) (ret []*v1beta1.ClusterServiceClass, err error)
	// Get retrieves the ClusterServiceClass from the index for a given name.
	Get(name string) (*v1beta1.ClusterServiceClass, error)
	ClusterServiceClassListerExpansion
}

// clusterServiceClassLister implements the ClusterServiceClassLister interface.
type clusterServiceClassLister struct {
	indexer cache.Indexer
}

// NewClusterServiceClassLister returns a new ClusterServiceClassLister.
func NewClusterServiceClassLister(indexer cache.Indexer) ClusterServiceClassLister {
	return &clusterServiceClassLister{indexer: indexer}
}

// List lists all ClusterServiceClasses in the indexer.
func (s *clusterServiceClassLister) List(selector labels.Selector) (ret []*v1beta1.ClusterServiceClass, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1beta1.ClusterServiceClass))
	})
	return ret, err
}

// Get retrieves the ClusterServiceClass from the index for a given name.
func (s *clusterServiceClassLister) Get(name string) (*v1beta1.ClusterServiceClass, error) {
	key := &v1beta1.ClusterServiceClass{ObjectMeta: v1.ObjectMeta{Name: name}}
	obj, exists, err := s.indexer.Get(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1beta1.Resource("clusterserviceclass"), name)
	}
	return obj.(*v1beta1.ClusterServiceClass), nil
}
