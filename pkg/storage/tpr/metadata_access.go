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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	accessor   = meta.NewAccessor()
	selfLinker = runtime.SelfLinker(accessor)
)

// GetAccessor returns a MetadataAccessor to fetch general information on metadata of
// runtime.Object types
func GetAccessor() meta.MetadataAccessor {
	return accessor
}

// GetNamespace returns the namespace for the given object, if there is one. If not, returns
// the empty string and a non-nil error
func GetNamespace(obj runtime.Object) (string, error) {
	return selfLinker.Namespace(obj)
}

func deletionTimestampExists(obj runtime.Object) (bool, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return false, err
	}
	t := accessor.GetDeletionTimestamp()
	return t != nil, nil
}

func deletionGracePeriodExists(obj runtime.Object) (bool, error) {
	objMeta, err := metav1.ObjectMetaFor(obj)
	if err != nil {
		return false, err
	}
	return objMeta.DeletionGracePeriodSeconds != nil, nil
}

func getFinalizers(obj runtime.Object) ([]string, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	return accessor.GetFinalizers(), nil
}

func addFinalizer(obj runtime.Object, value string) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	finalizers := append(accessor.GetFinalizers(), value)
	accessor.SetFinalizers(finalizers)
	return nil
}

func removeFinalizer(obj runtime.Object, value string) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	finalizers := accessor.GetFinalizers()
	newFinalizers := []string{}
	for _, finalizer := range finalizers {
		if finalizer == value {
			continue
		}
		newFinalizers = append(newFinalizers, finalizer)
	}
	accessor.SetFinalizers(newFinalizers)
	return nil
}
