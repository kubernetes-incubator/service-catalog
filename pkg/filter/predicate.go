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

package filter

import (
	"k8s.io/apimachinery/pkg/labels"
)

// Want to filter the allowed labels.
// want to use only
//const (
//	DoesNotExist Operator = "!"
//	Equals       Operator = "="
//	DoubleEquals Operator = "=="
//	In           Operator = "in"
//	NotEquals    Operator = "!="
//	NotIn        Operator = "notin"
//)

// A Predicate wraps a label.Selector allowing us to use selectors.
type Predicate interface {
	Matches(Fields) bool

	// Empty returns true if this predicate does not restrict the selection space.
	Empty() bool

	// String returns a human readable string that represents this selector.
	String() string
}

// NewSelector returns a nil selector
func NewPredicate() Predicate {
	return internalPredicate{}
}

type internalPredicate struct {
	selector labels.Selector
}

func (p internalPredicate) Matches(f Fields) bool {
	if p.Empty() {
		return true
	}
	return p.selector.Matches(f)
}

func (p internalPredicate) Empty() bool {
	if p.selector == nil {
		return true
	}
	return p.selector.Empty()
}

func (p internalPredicate) String() string {
	return p.selector.String()
}
