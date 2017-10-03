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
	internalversion "github.com/kubernetes-incubator/service-catalog/pkg/client/clientset_generated/internalclientset/typed/servicecatalog/internalversion"
	rest "k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
)

type FakeServicecatalog struct {
	*testing.Fake
}

func (c *FakeServicecatalog) ClusterServiceBrokers() internalversion.ClusterServiceBrokerInterface {
	return &FakeClusterServiceBrokers{c}
}

func (c *FakeServicecatalog) ServiceClasses() internalversion.ServiceClassInterface {
	return &FakeServiceClasses{c}
}

func (c *FakeServicecatalog) ServiceInstances(namespace string) internalversion.ServiceInstanceInterface {
	return &FakeServiceInstances{c, namespace}
}

func (c *FakeServicecatalog) ServiceInstanceCredentials(namespace string) internalversion.ServiceInstanceCredentialInterface {
	return &FakeServiceInstanceCredentials{c, namespace}
}

func (c *FakeServicecatalog) ServicePlans() internalversion.ServicePlanInterface {
	return &FakeServicePlans{c}
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *FakeServicecatalog) RESTClient() rest.Interface {
	var ret *rest.RESTClient
	return ret
}
