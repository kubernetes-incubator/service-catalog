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

package controller

import (
	"encoding/json"
	//"errors"
	"fmt"
	//"net/http"
	//"reflect"
	//"strings"
	"testing"
	//"time"
	//scmeta "github.com/kubernetes-incubator/service-catalog/pkg/api/meta"
	"github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/v1beta1"
	//v1beta1informers "github.com/kubernetes-incubator/service-catalog/pkg/client/informers_generated/externalversions/servicecatalog/v1beta1"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	fakeosb "github.com/pmorie/go-open-service-broker-client/v2/fake"
	corev1 "k8s.io/api/core/v1"
	//apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	//"k8s.io/apimachinery/pkg/util/diff"
	scfeatures "github.com/kubernetes-incubator/service-catalog/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	//"github.com/kubernetes-incubator/service-catalog/test/fake"
	//clientgofake "k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
)

// TestReconcileBindingWithParametersNamespacedRefs tests reconcileBinding to ensure a
// binding with parameters will be passed to the broker properly.
func TestReconcileServiceBindingWithParametersNamespacedRefs(t *testing.T) {
	err := utilfeature.DefaultFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.NamespacedServiceBroker))
	if err != nil {
		t.Fatalf("Could not enable NamespacedServiceBroker feature flag.")
	}
	defer utilfeature.DefaultFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.NamespacedServiceBroker))

	fakeKubeClient, fakeCatalogClient, fakeBrokerClient, testController, sharedInformers := newTestController(t, fakeosb.FakeClientConfiguration{
		BindReaction: &fakeosb.BindReaction{
			Response: &osb.BindResponse{
				Credentials: map[string]interface{}{
					"a": "b",
					"c": "d",
				},
			},
		},
	})

	addGetNamespaceReaction(fakeKubeClient)
	addGetSecretNotFoundReaction(fakeKubeClient)

	sharedInformers.ServiceBrokers().Informer().GetStore().Add(getTestServiceBroker())
	sharedInformers.ServiceClasses().Informer().GetStore().Add(getTestServiceClass())
	sharedInformers.ServicePlans().Informer().GetStore().Add(getTestServicePlan())
	sharedInformers.ServiceInstances().Informer().GetStore().Add(
		getTestServiceInstanceWithNamespacedRefsAndStatus(v1beta1.ConditionTrue))

	binding := &v1beta1.ServiceBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testServiceBindingName,
			Namespace:  testNamespace,
			Finalizers: []string{v1beta1.FinalizerServiceCatalog},
			Generation: 1,
		},
		Spec: v1beta1.ServiceBindingSpec{
			ServiceInstanceRef: v1beta1.LocalObjectReference{Name: testServiceInstanceName},
			ExternalID:         testServiceBindingGUID,
			SecretName:         testServiceBindingSecretName,
		},
		Status: v1beta1.ServiceBindingStatus{
			UnbindStatus: v1beta1.ServiceBindingUnbindStatusNotRequired,
		},
	}

	parameters := bindingParameters{Name: "test-param"}
	parameters.Args = append(parameters.Args, "first-arg")
	parameters.Args = append(parameters.Args, "second-arg")
	b, err := json.Marshal(parameters)
	if err != nil {
		t.Fatalf("Failed to marshal parameters %v : %v", parameters, err)
	}
	binding.Spec.Parameters = &runtime.RawExtension{Raw: b}

	if err := reconcileServiceBinding(t, testController, binding); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedParameters := map[string]interface{}{
		"args": []interface{}{
			"first-arg",
			"second-arg",
		},
		"name": "test-param",
	}
	expectedParametersChecksum := generateChecksumOfParametersOrFail(t, expectedParameters)

	binding = assertServiceBindingOperationInProgressWithParametersIsTheOnlyCatalogAction(t, fakeCatalogClient, binding, v1beta1.ServiceBindingOperationBind, expectedParameters, expectedParametersChecksum)
	fakeCatalogClient.ClearActions()

	assertGetNamespaceAction(t, fakeKubeClient.Actions())
	fakeKubeClient.ClearActions()

	assertNumberOfBrokerActions(t, fakeBrokerClient.Actions(), 0)

	err = reconcileServiceBinding(t, testController, binding)
	if err != nil {
		t.Fatalf("a valid binding should not fail: %v", err)
	}

	brokerActions := fakeBrokerClient.Actions()
	assertNumberOfBrokerActions(t, brokerActions, 1)
	assertBind(t, brokerActions[0], &osb.BindRequest{
		BindingID:  testServiceBindingGUID,
		InstanceID: testServiceInstanceGUID,
		ServiceID:  testServiceClassGUID,
		PlanID:     testServicePlanGUID,
		AppGUID:    strPtr(testNamespaceGUID),
		Parameters: map[string]interface{}{
			"args": []interface{}{
				"first-arg",
				"second-arg",
			},
			"name": "test-param",
		},
		BindResource: &osb.BindResource{
			AppGUID: strPtr(testNamespaceGUID),
		},
	})

	actions := fakeCatalogClient.Actions()
	assertNumberOfActions(t, actions, 1)

	updatedServiceBinding := assertUpdateStatus(t, actions[0], binding).(*v1beta1.ServiceBinding)
	assertServiceBindingOperationSuccessWithParameters(t, updatedServiceBinding, v1beta1.ServiceBindingOperationBind, expectedParameters, expectedParametersChecksum, binding)
	assertServiceBindingOrphanMitigationSet(t, updatedServiceBinding, false)

	kubeActions := fakeKubeClient.Actions()
	assertNumberOfActions(t, kubeActions, 3)
	assertActionEquals(t, kubeActions[0], "get", "namespaces")
	assertActionEquals(t, kubeActions[1], "get", "secrets")
	assertActionEquals(t, kubeActions[2], "create", "secrets")

	action := kubeActions[2].(clientgotesting.CreateAction)
	actionSecret, ok := action.GetObject().(*corev1.Secret)
	if !ok {
		t.Fatal("couldn't convert secret into a corev1.Secret")
	}
	controllerRef := metav1.GetControllerOf(actionSecret)
	if controllerRef == nil || controllerRef.UID != updatedServiceBinding.UID {
		t.Fatalf("Secret is not owned by the ServiceBinding: %v", controllerRef)
	}
	if !metav1.IsControlledBy(actionSecret, updatedServiceBinding) {
		t.Fatal("Secret is not owned by the ServiceBinding")
	}
	if e, a := testServiceBindingSecretName, actionSecret.Name; e != a {
		t.Fatalf("Unexpected name of secret; %s", expectedGot(e, a))
	}
	value, ok := actionSecret.Data["a"]
	if !ok {
		t.Fatal("Didn't find secret key 'a' in created secret")
	}
	if e, a := "b", string(value); e != a {
		t.Fatalf("Unexpected value of key 'a' in created secret; %s", expectedGot(e, a))
	}
	value, ok = actionSecret.Data["c"]
	if !ok {
		t.Fatal("Didn't find secret key 'c' in created secret")
	}
	if e, a := "d", string(value); e != a {
		t.Fatalf("Unexpected value of key 'c' in created secret; %s", expectedGot(e, a))
	}

	events := getRecordedEvents(testController)
	assertNumEvents(t, events, 1)

	expectedEvent := normalEventBuilder(successInjectedBindResultReason).msg(successInjectedBindResultMessage)
	if err := checkEvents(events, expectedEvent.stringArr()); err != nil {
		t.Fatal(err)
	}
}
