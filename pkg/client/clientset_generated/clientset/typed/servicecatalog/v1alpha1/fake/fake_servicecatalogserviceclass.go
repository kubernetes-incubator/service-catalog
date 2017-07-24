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

package fake

import (
	v1alpha1 "github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeServiceCatalogServiceClasses implements ServiceCatalogServiceClassInterface
type FakeServiceCatalogServiceClasses struct {
	Fake *FakeServicecatalogV1alpha1
}

var servicecatalogserviceclassesResource = schema.GroupVersionResource{Group: "servicecatalog.k8s.io", Version: "v1alpha1", Resource: "servicecatalogserviceclasses"}

var servicecatalogserviceclassesKind = schema.GroupVersionKind{Group: "servicecatalog.k8s.io", Version: "v1alpha1", Kind: "ServiceCatalogServiceClass"}

func (c *FakeServiceCatalogServiceClasses) Create(serviceCatalogServiceClass *v1alpha1.ServiceCatalogServiceClass) (result *v1alpha1.ServiceCatalogServiceClass, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootCreateAction(servicecatalogserviceclassesResource, serviceCatalogServiceClass), &v1alpha1.ServiceCatalogServiceClass{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.ServiceCatalogServiceClass), err
}

func (c *FakeServiceCatalogServiceClasses) Update(serviceCatalogServiceClass *v1alpha1.ServiceCatalogServiceClass) (result *v1alpha1.ServiceCatalogServiceClass, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootUpdateAction(servicecatalogserviceclassesResource, serviceCatalogServiceClass), &v1alpha1.ServiceCatalogServiceClass{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.ServiceCatalogServiceClass), err
}

func (c *FakeServiceCatalogServiceClasses) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewRootDeleteAction(servicecatalogserviceclassesResource, name), &v1alpha1.ServiceCatalogServiceClass{})
	return err
}

func (c *FakeServiceCatalogServiceClasses) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewRootDeleteCollectionAction(servicecatalogserviceclassesResource, listOptions)

	_, err := c.Fake.Invokes(action, &v1alpha1.ServiceCatalogServiceClassList{})
	return err
}

func (c *FakeServiceCatalogServiceClasses) Get(name string, options v1.GetOptions) (result *v1alpha1.ServiceCatalogServiceClass, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootGetAction(servicecatalogserviceclassesResource, name), &v1alpha1.ServiceCatalogServiceClass{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.ServiceCatalogServiceClass), err
}

func (c *FakeServiceCatalogServiceClasses) List(opts v1.ListOptions) (result *v1alpha1.ServiceCatalogServiceClassList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootListAction(servicecatalogserviceclassesResource, servicecatalogserviceclassesKind, opts), &v1alpha1.ServiceCatalogServiceClassList{})
	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.ServiceCatalogServiceClassList{}
	for _, item := range obj.(*v1alpha1.ServiceCatalogServiceClassList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested serviceCatalogServiceClasses.
func (c *FakeServiceCatalogServiceClasses) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewRootWatchAction(servicecatalogserviceclassesResource, opts))
}

// Patch applies the patch and returns the patched serviceCatalogServiceClass.
func (c *FakeServiceCatalogServiceClasses) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.ServiceCatalogServiceClass, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootPatchSubresourceAction(servicecatalogserviceclassesResource, name, data, subresources...), &v1alpha1.ServiceCatalogServiceClass{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.ServiceCatalogServiceClass), err
}
