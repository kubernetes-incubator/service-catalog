/*
Copyright 2016 The Kubernetes Authors.

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

package storage

import (
	"fmt"
	"log"
	"strings"

	model "github.com/kubernetes-incubator/service-catalog/model/service_controller"
)

type bindingPair struct {
	binding    *model.ServiceBinding
	credential *model.Credential
}

type memStorage struct {
	brokers map[string]*model.ServiceBroker
	// This gets fetched when a SB is created (or possibly later when refetched).
	// It's static for now to keep compatibility, seems like this could be more dynamic.
	catalogs map[string]*model.Catalog
	// maps instance ID to instance
	services map[string]*model.ServiceInstance
	// maps binding ID to binding
	// TODO: support looking up all bindings for a service instance.
	bindings map[string]*bindingPair
}

// CreateMemStorage creates an instance of Storage interface, backed by memory.
func CreateMemStorage() Storage {
	return &memStorage{
		brokers:  make(map[string]*model.ServiceBroker),
		catalogs: make(map[string]*model.Catalog),
		services: make(map[string]*model.ServiceInstance),
		bindings: make(map[string]*bindingPair),
	}
}

func (s *memStorage) GetInventory() (*model.Catalog, error) {
	services := []*model.Service{}
	for _, v := range s.catalogs {
		services = append(services, v.Services...)
	}
	return &model.Catalog{Services: services}, nil
}

func (s *memStorage) ListBrokers() ([]*model.ServiceBroker, error) {
	b := []*model.ServiceBroker{}
	for _, v := range s.brokers {
		b = append(b, v)
	}
	return b, nil
}

func (s *memStorage) GetBroker(id string) (*model.ServiceBroker, error) {
	if b, ok := s.brokers[id]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("No such broker: %s", id)
}

func (s *memStorage) AddBroker(broker *model.ServiceBroker, catalog *model.Catalog) error {
	if _, ok := s.brokers[broker.GUID]; ok {
		return fmt.Errorf("Broker %s already exists", broker.Name)
	}
	s.brokers[broker.GUID] = broker
	s.catalogs[broker.GUID] = catalog
	return nil
}

func (s *memStorage) UpdateBroker(broker *model.ServiceBroker, catalog *model.Catalog) error {
	if _, ok := s.brokers[broker.GUID]; !ok {
		return fmt.Errorf("Broker %s does not exist", broker.Name)
	}
	s.brokers[broker.GUID] = broker
	s.catalogs[broker.GUID] = catalog
	return nil
}

func (s *memStorage) DeleteBroker(id string) error {
	_, err := s.GetBroker(id)
	if err != nil {
		return fmt.Errorf("Broker %s does not exist", id)
	}
	delete(s.brokers, id)
	delete(s.catalogs, id)

	// TODO: Delete bindings too.
	return nil
}

func (s *memStorage) GetServiceClass(name string) (*model.Service, error) {
	c, err := s.GetInventory()
	if err != nil {
		return nil, err
	}
	for _, serviceType := range c.Services {
		if serviceType.Name == name {
			return serviceType, nil
		}
	}
	return nil, fmt.Errorf("ServiceType %s not found", name)
}

func (s *memStorage) ServiceInstanceExists(ns string, id string) bool {
	_, err := s.GetServiceInstance(ns, id)
	return err == nil
}

func (s *memStorage) ListServiceInstances(ns string) ([]*model.ServiceInstance, error) {
	services := []*model.ServiceInstance{}
	for _, v := range s.services {
		services = append(services, v)
	}
	return services, nil
}

func (s *memStorage) GetServiceInstance(ns string, id string) (*model.ServiceInstance, error) {
	service, ok := s.services[id]
	if !ok {
		return &model.ServiceInstance{}, fmt.Errorf("Service %s does not exist", id)
	}

	return service, nil
}

func (s *memStorage) AddServiceInstance(si *model.ServiceInstance) error {
	if s.ServiceInstanceExists("default", si.ID) {
		return fmt.Errorf("Service %s already exists", si.ID)
	}

	s.services[si.ID] = si
	return nil
}

func (s *memStorage) UpdateServiceInstance(si *model.ServiceInstance) error {
	s.services[si.ID] = si
	return nil
}

func (s *memStorage) DeleteServiceInstance(id string) error {
	// First delete all the bindings where this ID is either to / from
	bindings, err := GetBindingsForService(s, id, Both)
	for _, b := range bindings {
		err = s.DeleteServiceBinding(b.ID)
		if err != nil {
			return err
		}
	}
	delete(s.services, id)
	return nil
}

func (s *memStorage) ListServiceBindings() ([]*model.ServiceBinding, error) {
	bindings := []*model.ServiceBinding{}
	for _, v := range s.bindings {
		bindings = append(bindings, v.binding)
	}
	return bindings, nil
}

func (s *memStorage) GetServiceBinding(id string) (*model.ServiceBinding, error) {
	b, ok := s.bindings[id]
	if !ok {
		return &model.ServiceBinding{}, fmt.Errorf("Binding %s does not exist", id)
	}

	return b.binding, nil
}

func (s *memStorage) AddServiceBinding(binding *model.ServiceBinding, cred *model.Credential) error {
	_, err := s.GetServiceBinding(binding.ID)
	if err == nil {
		return fmt.Errorf("Binding %s already exists", binding.ID)
	}

	s.bindings[binding.ID] = &bindingPair{binding: binding, credential: cred}
	return nil
}

func (s *memStorage) UpdateServiceBinding(binding *model.ServiceBinding) error {
	_, err := s.GetServiceBinding(binding.ID)
	if err != nil {
		return fmt.Errorf("Binding %s doesn't exist", binding.ID)
	}

	// TODO(vaikas): Fix
	s.bindings[binding.ID] = &bindingPair{binding: binding, credential: nil}
	return nil
}

func (s *memStorage) DeleteServiceBinding(id string) error {
	log.Printf("Deleting binding: %s\n", id)
	delete(s.bindings, id)
	return nil
}

func (s *memStorage) getServiceInstanceByName(name string) (*model.ServiceInstance, error) {
	siList, err := s.ListServiceInstances("default")
	if err != nil {
		return nil, err
	}

	for _, si := range siList {
		if strings.Compare(si.Name, name) == 0 {
			return si, nil
		}
	}

	return nil, fmt.Errorf("Service instance %s was not found", name)
}
