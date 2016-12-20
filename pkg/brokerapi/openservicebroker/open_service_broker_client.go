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

package openservicebroker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	model "github.com/kubernetes-incubator/service-catalog/model/service_broker"
	scmodel "github.com/kubernetes-incubator/service-catalog/model/service_controller"
	"github.com/kubernetes-incubator/service-catalog/pkg/brokerapi"
	"github.com/kubernetes-incubator/service-catalog/pkg/controller/catalog/util"
)

const (
	catalogFormatString         = "%s/v2/catalog"
	serviceInstanceFormatString = "%s/v2/service_instances/%s"
	bindingFormatString         = "%s/v2/service_instances/%s/service_bindings/%s"

	httpTimeoutSeconds = 15
)

type openServiceBrokerClient struct {
	broker *scmodel.ServiceBroker
	client *http.Client
}

// NewClient creates an instance of BrokerClient for communicating with brokers
// which implement the Open Service Broker API.
func NewClient(b *scmodel.ServiceBroker) brokerapi.BrokerClient {
	return &openServiceBrokerClient{
		broker: b,
		client: &http.Client{
			Timeout: httpTimeoutSeconds * time.Second,
		},
	}
}

func (c *openServiceBrokerClient) GetCatalog() (*model.Catalog, error) {
	url := fmt.Sprintf(catalogFormatString, c.broker.BrokerURL)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.broker.AuthUsername, c.broker.AuthPassword)
	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("Failed to fetch catalog from %s\n%v", url, resp)
		log.Printf("err: %#v", err)
		return nil, err
	}

	var catalog model.Catalog
	if err = util.ResponseBodyToObject(resp, &catalog); err != nil {
		log.Printf("Failed to unmarshal catalog: %#v", err)
		return nil, err
	}

	return &catalog, nil
}

func (c *openServiceBrokerClient) CreateServiceInstance(ID string, req *model.ServiceInstanceRequest) (*model.ServiceInstance, error) {
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf(serviceInstanceFormatString, c.broker.BrokerURL, ID)

	// TODO: Handle the auth
	createHTTPReq, err := http.NewRequest("PUT", url, bytes.NewReader(jsonBytes))
	if err != nil {
		return nil, err
	}

	log.Printf("Doing a request to: %s", url)
	resp, err := c.client.Do(createHTTPReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// TODO: Align this with the actual response model.
	si := model.ServiceInstance{}
	if err := util.ResponseBodyToObject(resp, &si); err != nil {
		return nil, err
	}
	return &si, nil
}

func (c *openServiceBrokerClient) UpdateServiceInstance(ID string, req *model.ServiceInstanceRequest) (*model.ServiceInstance, error) {
	// TODO: https://github.com/kubernetes-incubator/service-catalog/issues/114
	return nil, fmt.Errorf("Not implemented")
}

func (c *openServiceBrokerClient) DeleteServiceInstance(ID string) error {
	url := fmt.Sprintf(serviceInstanceFormatString, c.broker.BrokerURL, ID)

	// TODO: Handle the auth
	deleteHTTPReq, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("Failed to create new HTTP request: %v", err)
		return err
	}

	log.Printf("Doing a request to: %s", url)
	resp, err := c.client.Do(deleteHTTPReq)
	if err != nil {
		log.Printf("Failed to DELETE: %#v", err)
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *openServiceBrokerClient) CreateServiceBinding(sID, bID string, req *model.BindingRequest) (*model.CreateServiceBindingResponse, error) {
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		log.Printf("Failed to marshal: %#v", err)
		return nil, err
	}

	url := fmt.Sprintf(bindingFormatString, c.broker.BrokerURL, sID, bID)

	// TODO: Handle the auth
	createHTTPReq, err := http.NewRequest("PUT", url, bytes.NewReader(jsonBytes))
	if err != nil {
		return nil, err
	}

	log.Printf("Doing a request to: %s", url)
	resp, err := c.client.Do(createHTTPReq)
	if err != nil {
		log.Printf("Failed to PUT: %#v", err)
		return nil, err
	}
	defer resp.Body.Close()

	sbr := model.CreateServiceBindingResponse{}
	err = util.ResponseBodyToObject(resp, &sbr)
	if err != nil {
		log.Printf("Failed to unmarshal: %#v", err)
		return nil, err
	}

	return &sbr, nil
}

func (c *openServiceBrokerClient) DeleteServiceBinding(sID, bID string) error {
	url := fmt.Sprintf(bindingFormatString, c.broker.BrokerURL, sID, bID)

	// TODO: Handle the auth
	deleteHTTPReq, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("Failed to create new HTTP request: %v", err)
		return err
	}

	log.Printf("Doing a request to: %s", url)
	resp, err := c.client.Do(deleteHTTPReq)
	if err != nil {
		log.Printf("Failed to DELETE: %#v", err)
		return err
	}
	defer resp.Body.Close()

	return nil
}
