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

package util

import (
	"net/http"
	"net/http/httptest"

	"github.com/gorilla/mux"
	"github.com/kubernetes-incubator/service-catalog/pkg/brokerapi"
	"github.com/kubernetes-incubator/service-catalog/pkg/util"
)

// FakeBrokerServer is a fake service broker server meant for testing that allows for customizing the response behavior
type FakeBrokerServer struct {
	responseStatus     int
	pollsRemaining     int
	shouldSucceedAsync bool
	operation          string
	server             *httptest.Server
}

// Start starts the fake broker server listening on a random port, passing back the server's URL
func (f *FakeBrokerServer) Start() string {
	router := mux.NewRouter()
	router.HandleFunc("/v2/catalog", f.catalogHandler).Methods("GET")
	router.HandleFunc("/v2/service_instances/{id}/last_operation", f.lastOperationHandler).Methods("GET")
	router.HandleFunc("/v2/service_instances/{id}", f.provisionHandler).Methods("PUT")
	router.HandleFunc("/v2/service_instances/{id}", f.updateHandler).Methods("PATCH")
	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}", f.bindHandler).Methods("PUT")
	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}", f.unbindHandler).Methods("DELETE")
	router.HandleFunc("/v2/service_instances/{id}", f.deprovisionHandler).Methods("DELETE")
	f.server = httptest.NewServer(router)
	return f.server.URL
}

// Stop shuts down the server
func (f *FakeBrokerServer) Stop() {
	f.server.Close()
}

// SetResponseStatus sets the default response status of the broker to the given HTTP status code
func (f *FakeBrokerServer) SetResponseStatus(status int) {
	f.responseStatus = status
}

// SetAsynchronous sets the number of polls before finished, final state, and operation for asynchronous operations
func (f *FakeBrokerServer) SetAsynchronous(numPolls int, shouldSucceed bool, operation string) {
	f.pollsRemaining = numPolls
	f.shouldSucceedAsync = shouldSucceed
	f.operation = operation
}

// HANDLERS

func (f *FakeBrokerServer) catalogHandler(w http.ResponseWriter, r *http.Request) {
	util.WriteResponse(w, http.StatusOK, &brokerapi.Catalog{})
}

func (f *FakeBrokerServer) lastOperationHandler(w http.ResponseWriter, r *http.Request) {
	req := &brokerapi.LastOperationRequest{}
	if err := util.BodyToObject(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var state string
	switch {
	case f.pollsRemaining > 0:
		f.pollsRemaining--
		state = brokerapi.StateInProgress
	case f.shouldSucceedAsync:
		state = brokerapi.StateSucceeded
	default:
		state = brokerapi.StateFailed
	}

	resp := brokerapi.LastOperationResponse{
		State: state,
	}
	util.WriteResponse(w, http.StatusOK, &resp)
}

func (f *FakeBrokerServer) provisionHandler(w http.ResponseWriter, r *http.Request) {
	req := &brokerapi.CreateServiceInstanceRequest{}
	if err := util.BodyToObject(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if !req.AcceptsIncomplete {
		// Synchronous
		util.WriteResponse(w, f.responseStatus, &brokerapi.CreateServiceInstanceResponse{})
	} else {
		// Asynchronous
		resp := brokerapi.CreateServiceInstanceResponse{
			Operation: f.operation,
		}
		util.WriteResponse(w, http.StatusAccepted, &resp)
	}
}

func (f *FakeBrokerServer) deprovisionHandler(w http.ResponseWriter, r *http.Request) {
	req := &brokerapi.DeleteServiceInstanceRequest{}
	if err := util.BodyToObject(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if !req.AcceptsIncomplete {
		// Synchronous
		util.WriteResponse(w, f.responseStatus, &brokerapi.DeleteServiceInstanceResponse{})
	} else {
		// Asynchronous
		resp := brokerapi.CreateServiceInstanceResponse{
			Operation: f.operation,
		}
		util.WriteResponse(w, http.StatusAccepted, &resp)
	}
}

func (f *FakeBrokerServer) updateHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement
	util.WriteResponse(w, http.StatusForbidden, nil)
}

func (f *FakeBrokerServer) bindHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement
	util.WriteResponse(w, http.StatusForbidden, nil)
}

func (f *FakeBrokerServer) unbindHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement
	util.WriteResponse(w, http.StatusForbidden, nil)
}
