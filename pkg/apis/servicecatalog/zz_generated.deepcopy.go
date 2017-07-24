// +build !ignore_autogenerated

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

// This file was autogenerated by deepcopy-gen. Do not edit it manually!

package servicecatalog

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	runtime "k8s.io/apimachinery/pkg/runtime"
	api_v1 "k8s.io/client-go/pkg/api/v1"
	reflect "reflect"
)

func init() {
	SchemeBuilder.Register(RegisterDeepCopies)
}

// RegisterDeepCopies adds deep-copy functions to the given scheme. Public
// to allow building arbitrary schemes.
func RegisterDeepCopies(scheme *runtime.Scheme) error {
	return scheme.AddGeneratedDeepCopyFuncs(
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_AlphaPodPresetTemplate, InType: reflect.TypeOf(&AlphaPodPresetTemplate{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_BindingCondition, InType: reflect.TypeOf(&BindingCondition{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_BrokerAuthInfo, InType: reflect.TypeOf(&BrokerAuthInfo{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_BrokerCondition, InType: reflect.TypeOf(&BrokerCondition{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_InstanceCondition, InType: reflect.TypeOf(&InstanceCondition{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBinding, InType: reflect.TypeOf(&ServiceCatalogBinding{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBindingList, InType: reflect.TypeOf(&ServiceCatalogBindingList{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBindingSpec, InType: reflect.TypeOf(&ServiceCatalogBindingSpec{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBindingStatus, InType: reflect.TypeOf(&ServiceCatalogBindingStatus{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBroker, InType: reflect.TypeOf(&ServiceCatalogBroker{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBrokerList, InType: reflect.TypeOf(&ServiceCatalogBrokerList{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBrokerSpec, InType: reflect.TypeOf(&ServiceCatalogBrokerSpec{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogBrokerStatus, InType: reflect.TypeOf(&ServiceCatalogBrokerStatus{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogInstance, InType: reflect.TypeOf(&ServiceCatalogInstance{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogInstanceList, InType: reflect.TypeOf(&ServiceCatalogInstanceList{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogInstanceSpec, InType: reflect.TypeOf(&ServiceCatalogInstanceSpec{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogInstanceStatus, InType: reflect.TypeOf(&ServiceCatalogInstanceStatus{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogServiceClass, InType: reflect.TypeOf(&ServiceCatalogServiceClass{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogServiceClassList, InType: reflect.TypeOf(&ServiceCatalogServiceClassList{})},
		conversion.GeneratedDeepCopyFunc{Fn: DeepCopy_servicecatalog_ServiceCatalogServicePlan, InType: reflect.TypeOf(&ServiceCatalogServicePlan{})},
	)
}

// DeepCopy_servicecatalog_AlphaPodPresetTemplate is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_AlphaPodPresetTemplate(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*AlphaPodPresetTemplate)
		out := out.(*AlphaPodPresetTemplate)
		*out = *in
		if newVal, err := c.DeepCopy(&in.Selector); err != nil {
			return err
		} else {
			out.Selector = *newVal.(*v1.LabelSelector)
		}
		return nil
	}
}

// DeepCopy_servicecatalog_BindingCondition is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_BindingCondition(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*BindingCondition)
		out := out.(*BindingCondition)
		*out = *in
		out.LastTransitionTime = in.LastTransitionTime.DeepCopy()
		return nil
	}
}

// DeepCopy_servicecatalog_BrokerAuthInfo is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_BrokerAuthInfo(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*BrokerAuthInfo)
		out := out.(*BrokerAuthInfo)
		*out = *in
		if in.BasicAuthSecret != nil {
			in, out := &in.BasicAuthSecret, &out.BasicAuthSecret
			*out = new(api_v1.ObjectReference)
			**out = **in
		}
		return nil
	}
}

// DeepCopy_servicecatalog_BrokerCondition is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_BrokerCondition(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*BrokerCondition)
		out := out.(*BrokerCondition)
		*out = *in
		out.LastTransitionTime = in.LastTransitionTime.DeepCopy()
		return nil
	}
}

// DeepCopy_servicecatalog_InstanceCondition is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_InstanceCondition(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*InstanceCondition)
		out := out.(*InstanceCondition)
		*out = *in
		out.LastTransitionTime = in.LastTransitionTime.DeepCopy()
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBinding is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBinding(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBinding)
		out := out.(*ServiceCatalogBinding)
		*out = *in
		if newVal, err := c.DeepCopy(&in.ObjectMeta); err != nil {
			return err
		} else {
			out.ObjectMeta = *newVal.(*v1.ObjectMeta)
		}
		if err := DeepCopy_servicecatalog_ServiceCatalogBindingSpec(&in.Spec, &out.Spec, c); err != nil {
			return err
		}
		if err := DeepCopy_servicecatalog_ServiceCatalogBindingStatus(&in.Status, &out.Status, c); err != nil {
			return err
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBindingList is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBindingList(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBindingList)
		out := out.(*ServiceCatalogBindingList)
		*out = *in
		if in.Items != nil {
			in, out := &in.Items, &out.Items
			*out = make([]ServiceCatalogBinding, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_ServiceCatalogBinding(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBindingSpec is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBindingSpec(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBindingSpec)
		out := out.(*ServiceCatalogBindingSpec)
		*out = *in
		if in.Parameters != nil {
			in, out := &in.Parameters, &out.Parameters
			if newVal, err := c.DeepCopy(*in); err != nil {
				return err
			} else {
				*out = newVal.(*runtime.RawExtension)
			}
		}
		if in.AlphaPodPresetTemplate != nil {
			in, out := &in.AlphaPodPresetTemplate, &out.AlphaPodPresetTemplate
			*out = new(AlphaPodPresetTemplate)
			if err := DeepCopy_servicecatalog_AlphaPodPresetTemplate(*in, *out, c); err != nil {
				return err
			}
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBindingStatus is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBindingStatus(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBindingStatus)
		out := out.(*ServiceCatalogBindingStatus)
		*out = *in
		if in.Conditions != nil {
			in, out := &in.Conditions, &out.Conditions
			*out = make([]BindingCondition, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_BindingCondition(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		if in.Checksum != nil {
			in, out := &in.Checksum, &out.Checksum
			*out = new(string)
			**out = **in
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBroker is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBroker(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBroker)
		out := out.(*ServiceCatalogBroker)
		*out = *in
		if newVal, err := c.DeepCopy(&in.ObjectMeta); err != nil {
			return err
		} else {
			out.ObjectMeta = *newVal.(*v1.ObjectMeta)
		}
		if err := DeepCopy_servicecatalog_ServiceCatalogBrokerSpec(&in.Spec, &out.Spec, c); err != nil {
			return err
		}
		if err := DeepCopy_servicecatalog_ServiceCatalogBrokerStatus(&in.Status, &out.Status, c); err != nil {
			return err
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBrokerList is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBrokerList(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBrokerList)
		out := out.(*ServiceCatalogBrokerList)
		*out = *in
		if in.Items != nil {
			in, out := &in.Items, &out.Items
			*out = make([]ServiceCatalogBroker, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_ServiceCatalogBroker(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBrokerSpec is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBrokerSpec(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBrokerSpec)
		out := out.(*ServiceCatalogBrokerSpec)
		*out = *in
		if in.AuthInfo != nil {
			in, out := &in.AuthInfo, &out.AuthInfo
			*out = new(BrokerAuthInfo)
			if err := DeepCopy_servicecatalog_BrokerAuthInfo(*in, *out, c); err != nil {
				return err
			}
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogBrokerStatus is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogBrokerStatus(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogBrokerStatus)
		out := out.(*ServiceCatalogBrokerStatus)
		*out = *in
		if in.Conditions != nil {
			in, out := &in.Conditions, &out.Conditions
			*out = make([]BrokerCondition, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_BrokerCondition(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		if in.Checksum != nil {
			in, out := &in.Checksum, &out.Checksum
			*out = new(string)
			**out = **in
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogInstance is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogInstance(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogInstance)
		out := out.(*ServiceCatalogInstance)
		*out = *in
		if newVal, err := c.DeepCopy(&in.ObjectMeta); err != nil {
			return err
		} else {
			out.ObjectMeta = *newVal.(*v1.ObjectMeta)
		}
		if err := DeepCopy_servicecatalog_ServiceCatalogInstanceSpec(&in.Spec, &out.Spec, c); err != nil {
			return err
		}
		if err := DeepCopy_servicecatalog_ServiceCatalogInstanceStatus(&in.Status, &out.Status, c); err != nil {
			return err
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogInstanceList is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogInstanceList(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogInstanceList)
		out := out.(*ServiceCatalogInstanceList)
		*out = *in
		if in.Items != nil {
			in, out := &in.Items, &out.Items
			*out = make([]ServiceCatalogInstance, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_ServiceCatalogInstance(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogInstanceSpec is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogInstanceSpec(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogInstanceSpec)
		out := out.(*ServiceCatalogInstanceSpec)
		*out = *in
		if in.Parameters != nil {
			in, out := &in.Parameters, &out.Parameters
			if newVal, err := c.DeepCopy(*in); err != nil {
				return err
			} else {
				*out = newVal.(*runtime.RawExtension)
			}
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogInstanceStatus is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogInstanceStatus(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogInstanceStatus)
		out := out.(*ServiceCatalogInstanceStatus)
		*out = *in
		if in.Conditions != nil {
			in, out := &in.Conditions, &out.Conditions
			*out = make([]InstanceCondition, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_InstanceCondition(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		if in.LastOperation != nil {
			in, out := &in.LastOperation, &out.LastOperation
			*out = new(string)
			**out = **in
		}
		if in.DashboardURL != nil {
			in, out := &in.DashboardURL, &out.DashboardURL
			*out = new(string)
			**out = **in
		}
		if in.Checksum != nil {
			in, out := &in.Checksum, &out.Checksum
			*out = new(string)
			**out = **in
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogServiceClass is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogServiceClass(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogServiceClass)
		out := out.(*ServiceCatalogServiceClass)
		*out = *in
		if newVal, err := c.DeepCopy(&in.ObjectMeta); err != nil {
			return err
		} else {
			out.ObjectMeta = *newVal.(*v1.ObjectMeta)
		}
		if in.Plans != nil {
			in, out := &in.Plans, &out.Plans
			*out = make([]ServiceCatalogServicePlan, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_ServiceCatalogServicePlan(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		if in.ExternalMetadata != nil {
			in, out := &in.ExternalMetadata, &out.ExternalMetadata
			if newVal, err := c.DeepCopy(*in); err != nil {
				return err
			} else {
				*out = newVal.(*runtime.RawExtension)
			}
		}
		if in.AlphaTags != nil {
			in, out := &in.AlphaTags, &out.AlphaTags
			*out = make([]string, len(*in))
			copy(*out, *in)
		}
		if in.AlphaRequires != nil {
			in, out := &in.AlphaRequires, &out.AlphaRequires
			*out = make([]string, len(*in))
			copy(*out, *in)
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogServiceClassList is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogServiceClassList(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogServiceClassList)
		out := out.(*ServiceCatalogServiceClassList)
		*out = *in
		if in.Items != nil {
			in, out := &in.Items, &out.Items
			*out = make([]ServiceCatalogServiceClass, len(*in))
			for i := range *in {
				if err := DeepCopy_servicecatalog_ServiceCatalogServiceClass(&(*in)[i], &(*out)[i], c); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// DeepCopy_servicecatalog_ServiceCatalogServicePlan is an autogenerated deepcopy function.
func DeepCopy_servicecatalog_ServiceCatalogServicePlan(in interface{}, out interface{}, c *conversion.Cloner) error {
	{
		in := in.(*ServiceCatalogServicePlan)
		out := out.(*ServiceCatalogServicePlan)
		*out = *in
		if in.Bindable != nil {
			in, out := &in.Bindable, &out.Bindable
			*out = new(bool)
			**out = **in
		}
		if in.ExternalMetadata != nil {
			in, out := &in.ExternalMetadata, &out.ExternalMetadata
			if newVal, err := c.DeepCopy(*in); err != nil {
				return err
			} else {
				*out = newVal.(*runtime.RawExtension)
			}
		}
		if in.AlphaInstanceCreateParameterSchema != nil {
			in, out := &in.AlphaInstanceCreateParameterSchema, &out.AlphaInstanceCreateParameterSchema
			if newVal, err := c.DeepCopy(*in); err != nil {
				return err
			} else {
				*out = newVal.(*runtime.RawExtension)
			}
		}
		if in.AlphaInstanceUpdateParameterSchema != nil {
			in, out := &in.AlphaInstanceUpdateParameterSchema, &out.AlphaInstanceUpdateParameterSchema
			if newVal, err := c.DeepCopy(*in); err != nil {
				return err
			} else {
				*out = newVal.(*runtime.RawExtension)
			}
		}
		if in.AlphaBindingCreateParameterSchema != nil {
			in, out := &in.AlphaBindingCreateParameterSchema, &out.AlphaBindingCreateParameterSchema
			if newVal, err := c.DeepCopy(*in); err != nil {
				return err
			} else {
				*out = newVal.(*runtime.RawExtension)
			}
		}
		return nil
	}
}
