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

package validation

// this contains stubs of nothing until it is understood how it works

import (
	// commented out until we use the base validation utilities

	apivalidation "k8s.io/kubernetes/pkg/api/validation"
	// "k8s.io/kubernetes/pkg/api/validation/path"
	// utilvalidation "k8s.io/kubernetes/pkg/util/validation"
	"k8s.io/kubernetes/pkg/util/validation/field"

	sc "github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog"
)

// assuming a nil non-error is ok. not okay, should be empty struct `field.ErrorList{}`

// ValidateBroker makes sure a broker object is okay?
func ValidateBroker(broker *sc.Broker) field.ErrorList {
	allErrs := field.ErrorList{}
	// validate the name?
	allErrs = append(allErrs, apivalidation.ValidateObjectMeta(&broker.ObjectMeta, false, /*namespace*/
		apivalidation.ValidateReplicationControllerName, // our custom name validator?
		field.NewPath("metadata"))...)
	allErrs = append(allErrs, validateBrokerSpec(&broker.Spec, field.NewPath("Spec"))...)
	// Do we need to validate the status array?
	// allErrs = append(allErrs, validateBrokerStatus(&broker.Spec, field.NewPath("Status"))...)
	return allErrs
}

func validateBrokerSpec(spec *sc.BrokerSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	/* This is what is in the broker spec.
	URL string
	AuthUsername string
	AuthPassword string
	OSBGUID string
	*/

	if "" == spec.URL {
		allErrs = append(allErrs,
			field.Required(fldPath.Child("URL"),
				"brokers must have a remote url to contact"))
	}
	// xor user and pass, must have both or none, not either
	hasUser := "" != spec.AuthUsername
	hasPassword := "" != spec.AuthPassword
	if (hasUser || hasPassword) && !(hasUser && hasPassword) {
		if hasPassword {
			allErrs = append(allErrs,
				field.Required(
					fldPath.Child("AuthUsername"),
					"must have username in addition to password"))
		} else if hasUser {
			allErrs = append(allErrs,
				field.Required(
					fldPath.Child("AuthPassword"),
					"must have password in addition to username"))
		}
	}
	// spec.OSBGUID has no properties to validate
	return allErrs
}

// ValidateBrokerUpdate checks that when changing from an older broker to a newer broker is okay ?
func ValidateBrokerUpdate(new *sc.Broker, old *sc.Broker) field.ErrorList {
	allErrs := field.ErrorList{}
	// should each individual broker validate successfully before validating changes?
	allErrs = append(allErrs, ValidateBroker(new)...)
	allErrs = append(allErrs, ValidateBroker(old)...)
	// allErrs = append(allErrs, validateObjectMetaUpdate(new, old)...)
	// allErrs = append(allErrs, validateBrokerSpecUpdate(new, old)...)
	// allErrs = append(allErrs, validateBrokerStatusUpdate(new, old)...)
	return allErrs
}
