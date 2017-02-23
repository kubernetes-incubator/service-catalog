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

package tpr

import (
	"encoding/json"

	"k8s.io/kubernetes/pkg/runtime"
)

func JSONBytesToObj(in []byte, out runtime.Object) error {
	if err := json.Unmarshal(in, out); err != nil {
		return err
	}
	return nil
}

// FromUnstructured converts o, a Kubernetes Third Party Resource type, into a
// *runtime.Unstructured and writes it to object. Returns a non-nil error is there were any issues
// with the conversion
func FromUnstructured(in *runtime.Unstructured, out runtime.Object) error {
	b, err := json.Marshal(in.Object)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(b, out); err != nil {
		return err
	}
	return nil
}

// FromUnstructuredList converts a list of *runtime.Unstructured into a runtime.Object
func FromUnstructuredList(in *runtime.UnstructuredList, out runtime.Object) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return err
	}
	return nil
}

// ToUnstructured converts in (which should be a Service Catalog third party object type) into a
// runtime.Unstructured for use in writing to Kubernetes
func ToUnstructured(in runtime.Object) (*runtime.Unstructured, error) {
	m, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var ret runtime.Unstructured
	err = json.Unmarshal(m, &ret)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}
