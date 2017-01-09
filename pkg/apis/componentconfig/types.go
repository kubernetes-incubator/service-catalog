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

// The controller is responsible for running control loops that reconcile
// the state of service catalog API resources with service brokers, service
// classes, service instances, and service bindings.

package componentconfig

// ControllerConfiguration encapsulates all controller configuration.
type ControllerConfiguration struct {
	// Address is the IP address to serve on (set to 0.0.0.0 for all interfaces).
	Address string
	// Port is the port that the controller's http service runs on.
	Port int32
	// KubeconfigPath is the path to the kubeconfig file with authorization
	// information.
	KubeconfigPath string
}
