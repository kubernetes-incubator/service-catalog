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

package model

// ServiceBinding contains all of the info about a particular binding
// (or link) between an app as a service instance.
type ServiceBinding struct {
	ID                string `json:"id"`
	ServiceID         string `json:"service_id"`
	AppID             string `json:"app_id"`
	ServicePlanID     string `json:"service_plan_id"`
	PrivateKey        string `json:"private_key"`
	ServiceInstanceID string `json:"service_instance_id"`
}

// BindingRequest is the request sent a service broker asking for a new
// binding to be created.
type BindingRequest struct {
	AppGUID      string                 `json:"app_guid,omitempty"`
	PlanID       string                 `json:"plan_id,omitempty"`
	ServiceID    string                 `json:"service_id,omitempty"`
	BindResource map[string]interface{} `json:"bind_resource,omitempty"`
	Parameters   map[string]interface{} `json:"parameters,omitempty"`
}

// CreateServiceBindingResponse is the response msg from a BindingRequest.
type CreateServiceBindingResponse struct {
	// SyslogDrainURL string      `json:"syslog_drain_url, omitempty"`
	Credentials Credential `json:"credentials"`
}

// Credential contains some of the popular (but not required) types of
// crednetial data that might be included in a binding.
type Credential struct {
	Hostname string `json:"hostname"`
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}
