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
	"fmt"
	"net/http"
	"testing"

	osb "github.com/kubernetes-sigs/go-open-service-broker-client/v2"
	fakeosb "github.com/kubernetes-sigs/go-open-service-broker-client/v2/fake"
	"github.com/kubernetes-sigs/service-catalog/pkg/apis/servicecatalog/v1beta1"
	v1beta1informers "github.com/kubernetes-sigs/service-catalog/pkg/client/informers_generated/externalversions/servicecatalog/v1beta1"
	scfeatures "github.com/kubernetes-sigs/service-catalog/pkg/features"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientgofake "k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
)

// TestReconcileBindingWithParametersNamespacedRefs tests reconcileBinding to ensure a
// binding with parameters will be passed to the broker properly.
func TestReconcileServiceBindingWithParametersNamespacedRefs(t *testing.T) {
	err := utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.NamespacedServiceBroker))
	if err != nil {
		t.Fatalf("Could not enable NamespacedServiceBroker feature flag.")
	}
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.NamespacedServiceBroker))

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

	key := testServiceBindingSecretKey
	binding := &v1beta1.ServiceBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testServiceBindingName,
			Namespace:  testNamespace,
			Finalizers: []string{v1beta1.FinalizerServiceCatalog},
			Generation: 1,
		},
		Spec: v1beta1.ServiceBindingSpec{
			InstanceRef: v1beta1.LocalObjectReference{Name: testServiceInstanceName},
			ExternalID:  testServiceBindingGUID,
			SecretName:  testServiceBindingSecretName,
			SecretKey:   &key,
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
		Context: testContext,
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

	credentials_json, ok := actionSecret.Data[key]
	if !ok {
		t.Fatalf("Didn't find secret key '%s' in created secret", key)
	}
	credentials := map[string]string{}

	if err := json.Unmarshal(credentials_json, &credentials); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	actual, ok := credentials["a"]
	if !ok {
		t.Fatal("Didn't find secret key 'a' in created secret")
	}
	if e := "b"; actual != e {
		t.Fatalf("Unexpected value of key 'a' in created secret; %s", expectedGot(e, actual))
	}
	actual, ok = credentials["c"]
	if !ok {
		t.Fatal("Didn't find secret key 'c' in created secret")
	}
	if e := "d"; actual != e {
		t.Fatalf("Unexpected value of key 'c' in created secret; %s", expectedGot(e, actual))
	}

	events := getRecordedEvents(testController)
	assertNumEvents(t, events, 1)

	expectedEvent := normalEventBuilder(successInjectedBindResultReason).msg(successInjectedBindResultMessage)
	if err := checkEvents(events, expectedEvent.stringArr()); err != nil {
		t.Fatal(err)
	}
}

// TestReconcileServiceBindingAsynchronousBindNamespacedRefs tests the situation where the
// controller receives an asynchronous bind response back from the broker when
// doing a bind call.
func TestReconcileServiceBindingAsynchronousBindNamespacedRefs(t *testing.T) {
	err := utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.NamespacedServiceBroker))
	if err != nil {
		t.Fatalf("Could not enable NamespacedServiceBroker feature flag.")
	}
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.NamespacedServiceBroker))

	utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.AsyncBindingOperations))
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.AsyncBindingOperations))

	key := osb.OperationKey(testOperation)
	fakeKubeClient, fakeCatalogClient, fakeServiceBrokerClient, testController, sharedInformers := newTestController(t, fakeosb.FakeClientConfiguration{
		BindReaction: &fakeosb.BindReaction{
			Response: &osb.BindResponse{
				Async:        true,
				OperationKey: &key,
			},
		},
	})

	addGetNamespaceReaction(fakeKubeClient)
	addGetSecretNotFoundReaction(fakeKubeClient)

	sharedInformers.ServiceBrokers().Informer().GetStore().Add(getTestServiceBroker())
	sharedInformers.ServiceClasses().Informer().GetStore().Add(getTestBindingRetrievableServiceClass())
	sharedInformers.ServicePlans().Informer().GetStore().Add(getTestServicePlan())
	sharedInformers.ServiceInstances().Informer().GetStore().Add(getTestServiceInstanceWithNamespacedRefsAndStatus(v1beta1.ConditionTrue))

	binding := getTestServiceBinding()
	bindingKey := binding.Namespace + "/" + binding.Name

	if testController.bindingPollingQueue.NumRequeues(bindingKey) != 0 {
		t.Fatalf("Expected polling queue to not have any record of test binding")
	}

	if err := reconcileServiceBinding(t, testController, binding); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	binding = assertServiceBindingBindInProgressIsTheOnlyCatalogAction(t, fakeCatalogClient, binding)
	fakeCatalogClient.ClearActions()

	assertGetNamespaceAction(t, fakeKubeClient.Actions())
	fakeKubeClient.ClearActions()

	assertNumberOfBrokerActions(t, fakeServiceBrokerClient.Actions(), 0)

	if err := reconcileServiceBinding(t, testController, binding); err != nil {
		t.Fatalf("a valid binding should not fail: %v", err)
	}

	if testController.bindingPollingQueue.NumRequeues(bindingKey) != 1 {
		t.Fatalf("Expected polling queue to have a record of seeing test binding once")
	}
}

// TestReconcileBindingDeleteNamespacedRefs tests reconcileBinding to ensure a
// binding deletion works as expected.
func TestReconcileServiceBindingDeleteNamespacedRefs(t *testing.T) {
	err := utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.NamespacedServiceBroker))
	if err != nil {
		t.Fatalf("Could not enable NamespacedServiceBroker feature flag.")
	}
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.NamespacedServiceBroker))

	cases := []struct {
		name     string
		instance *v1beta1.ServiceInstance
		binding  *v1beta1.ServiceBinding
	}{
		{
			name:     "normal binding",
			instance: getTestServiceInstanceWithNamespacedRefsAndExternalProperties(),
			binding: &v1beta1.ServiceBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:              testServiceBindingName,
					Namespace:         testNamespace,
					DeletionTimestamp: &metav1.Time{},
					Finalizers:        []string{v1beta1.FinalizerServiceCatalog},
					Generation:        2,
				},
				Spec: v1beta1.ServiceBindingSpec{
					InstanceRef: v1beta1.LocalObjectReference{Name: testServiceInstanceName},
					ExternalID:  testServiceBindingGUID,
					SecretName:  testServiceBindingSecretName,
				},
				Status: v1beta1.ServiceBindingStatus{
					ReconciledGeneration: 1,
					ExternalProperties:   &v1beta1.ServiceBindingPropertiesState{},
					UnbindStatus:         v1beta1.ServiceBindingUnbindStatusRequired,
				},
			},
		},
		{
			name: "binding with instance pointing to non-existent plan",
			instance: &v1beta1.ServiceInstance{
				ObjectMeta: metav1.ObjectMeta{Name: testServiceInstanceName, Namespace: testNamespace},
				Spec: v1beta1.ServiceInstanceSpec{
					ExternalID:      testServiceInstanceGUID,
					ServiceClassRef: &v1beta1.LocalObjectReference{Name: testServiceClassGUID},
					ServicePlanRef:  nil,
					PlanReference: v1beta1.PlanReference{
						ServiceClassExternalName: testServiceClassName,
						ServicePlanExternalName:  testNonExistentServicePlanName,
					},
				},
				Status: v1beta1.ServiceInstanceStatus{
					ExternalProperties: &v1beta1.ServiceInstancePropertiesState{
						ServicePlanExternalID:   testServicePlanGUID,
						ServicePlanExternalName: testServicePlanName,
					},
				},
			},
			binding: &v1beta1.ServiceBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:              testServiceBindingName,
					Namespace:         testNamespace,
					DeletionTimestamp: &metav1.Time{},
					Finalizers:        []string{v1beta1.FinalizerServiceCatalog},
					Generation:        2,
				},
				Spec: v1beta1.ServiceBindingSpec{
					InstanceRef: v1beta1.LocalObjectReference{Name: testServiceInstanceName},
					ExternalID:  testServiceBindingGUID,
					SecretName:  testServiceBindingSecretName,
				},
				Status: v1beta1.ServiceBindingStatus{
					ReconciledGeneration: 1,
					ExternalProperties:   &v1beta1.ServiceBindingPropertiesState{},
					UnbindStatus:         v1beta1.ServiceBindingUnbindStatusRequired,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakeKubeClient, fakeCatalogClient, fakeBrokerClient, testController, sharedInformers := newTestController(t, fakeosb.FakeClientConfiguration{
				UnbindReaction: &fakeosb.UnbindReaction{
					Response: &osb.UnbindResponse{},
				},
			})

			sharedInformers.ServiceBrokers().Informer().GetStore().Add(getTestServiceBroker())
			sharedInformers.ServiceClasses().Informer().GetStore().Add(getTestServiceClass())
			sharedInformers.ServicePlans().Informer().GetStore().Add(getTestServicePlan())
			sharedInformers.ServiceInstances().Informer().GetStore().Add(tc.instance)

			binding := tc.binding
			fakeCatalogClient.AddReactor("get", "servicebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
				return true, binding, nil
			})

			if err := reconcileServiceBinding(t, testController, binding); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			binding = assertServiceBindingUnbindInProgressIsTheOnlyCatalogAction(t, fakeCatalogClient, binding)
			fakeCatalogClient.ClearActions()

			assertDeleteSecretAction(t, fakeKubeClient.Actions(), binding.Spec.SecretName)
			fakeKubeClient.ClearActions()

			assertNumberOfBrokerActions(t, fakeBrokerClient.Actions(), 0)

			err := reconcileServiceBinding(t, testController, binding)
			if err != nil {
				t.Fatalf("%v", err)
			}

			brokerActions := fakeBrokerClient.Actions()
			assertNumberOfBrokerActions(t, brokerActions, 1)
			assertUnbind(t, brokerActions[0], &osb.UnbindRequest{
				BindingID:  testServiceBindingGUID,
				InstanceID: testServiceInstanceGUID,
				ServiceID:  testServiceClassGUID,
				PlanID:     testServicePlanGUID,
			})

			kubeActions := fakeKubeClient.Actions()
			// The action should be deleting the secret
			assertNumberOfActions(t, kubeActions, 1)
			assertActionEquals(t, kubeActions[0], "delete", "secrets")

			deleteAction := kubeActions[0].(clientgotesting.DeleteActionImpl)
			if e, a := binding.Spec.SecretName, deleteAction.Name; e != a {
				t.Fatalf("Unexpected name of secret: %s", expectedGot(e, a))
			}

			actions := fakeCatalogClient.Actions()
			// The actions should be:
			// 0. Updating the ready condition
			// 1. Removing finalizer
			assertNumberOfActions(t, actions, 2)

			assertUpdateStatus(t, actions[0], binding)
			updatedServiceBinding := assertUpdate(t, actions[1], binding)
			assertServiceBindingOperationSuccess(t, updatedServiceBinding, v1beta1.ServiceBindingOperationUnbind, binding)
			assertServiceBindingOrphanMitigationSet(t, updatedServiceBinding, false)

			events := getRecordedEvents(testController)

			expectedEvent := normalEventBuilder(successUnboundReason)
			if err := checkEventPrefixes(events, expectedEvent.stringArr()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPollServiceBindingNamespacedRefs(t *testing.T) {
	utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.NamespacedServiceBroker))
	utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.AsyncBindingOperations))
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.NamespacedServiceBroker))
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.AsyncBindingOperations))

	goneError := osb.HTTPStatusCodeError{
		StatusCode: http.StatusGone,
	}

	validatePollBindingLastOperationAction := func(t *testing.T, actions []fakeosb.Action) {
		assertNumberOfBrokerActions(t, actions, 1)

		operationKey := osb.OperationKey(testOperation)
		assertPollBindingLastOperation(t, actions[0], &osb.BindingLastOperationRequest{
			InstanceID:   testServiceInstanceGUID,
			BindingID:    testServiceBindingGUID,
			ServiceID:    strPtr(testServiceClassGUID),
			PlanID:       strPtr(testServicePlanGUID),
			OperationKey: &operationKey,
		})
	}

	validatePollBindingLastOperationAndGetBindingActions := func(t *testing.T, actions []fakeosb.Action) {
		assertNumberOfBrokerActions(t, actions, 2)

		operationKey := osb.OperationKey(testOperation)
		assertPollBindingLastOperation(t, actions[0], &osb.BindingLastOperationRequest{
			InstanceID:   testServiceInstanceGUID,
			BindingID:    testServiceBindingGUID,
			ServiceID:    strPtr(testServiceClassGUID),
			PlanID:       strPtr(testServicePlanGUID),
			OperationKey: &operationKey,
		})

		assertGetBinding(t, actions[1], &osb.GetBindingRequest{
			InstanceID: testServiceInstanceGUID,
			BindingID:  testServiceBindingGUID,
		})
	}

	cases := []struct {
		name                       string
		binding                    *v1beta1.ServiceBinding
		pollReaction               *fakeosb.PollBindingLastOperationReaction
		getBindingReaction         *fakeosb.GetBindingReaction
		environmentSetupFunc       func(t *testing.T, fakeKubeClient *clientgofake.Clientset, sharedInformers v1beta1informers.Interface)
		validateBrokerActionsFunc  func(t *testing.T, actions []fakeosb.Action)
		validateKubeActionsFunc    func(t *testing.T, actions []clientgotesting.Action)
		assertPerfomerdActionsFunc func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding)
		shouldError                bool
		shouldFinishPolling        bool
		expectedEvents             []string
	}{
		// Bind
		{
			name:    "bind - error",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Error: fmt.Errorf("random error"),
			},
			validateBrokerActionsFunc:  validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: nil, // does not update resources
			shouldFinishPolling:        false,
			expectedEvents:             []string{corev1.EventTypeWarning + " " + errorPollingLastOperationReason + " " + "Error polling last operation: random error"},
		},
		{
			// Special test for 410, as it is treated differently in other operations
			name:    "bind - 410 Gone considered error",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Error: goneError,
			},
			validateBrokerActionsFunc:  validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: nil, // does not update resources
			shouldFinishPolling:        false,
			expectedEvents:             []string{corev1.EventTypeWarning + " " + errorPollingLastOperationReason + " " + "Error polling last operation: " + goneError.Error()},
		},
		{
			name:    "bind - in progress",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateInProgress,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncInProgress(t, updatedBinding, v1beta1.ServiceBindingOperationBind, asyncBindingReason, testOperation, originalBinding)
			},
			shouldFinishPolling: false,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + asyncBindingReason + " " + "The binding is being created asynchronously (testdescr)"},
		},
		{
			name:    "bind - failed",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateFailed,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingRequestFailingError(
					t,
					updatedBinding,
					v1beta1.ServiceBindingOperationBind,
					errorBindCallReason,
					errorBindCallReason,
					originalBinding,
				)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorBindCallReason + " " + "Bind call failed: " + lastOperationDescription,
				corev1.EventTypeWarning + " " + errorBindCallReason + " " + "Bind call failed: " + lastOperationDescription,
			},
		},
		{
			name:    "bind - invalid state",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       "test invalid state",
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc:  validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: nil, // does not update resources
			shouldFinishPolling:        false,
			expectedEvents:             []string{}, // does not record event
		},
		{
			name:    "bind - in progress - retry duration exceeded",
			binding: getTestServiceBindingAsyncBindingRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateInProgress,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncBindRetryDurationExceeded(t, updatedBinding, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorAsyncOpTimeoutReason + " " + "The asynchronous Bind operation timed out and will not be retried",
				corev1.EventTypeWarning + " " + errorReconciliationRetryTimeoutReason + " " + "Stopping reconciliation retries because too much time has elapsed",
				corev1.EventTypeWarning + " " + errorServiceBindingOrphanMitigation + " " + "Starting orphan mitigation",
			},
		},
		{
			name:    "bind - invalid state - retry duration exceeded",
			binding: getTestServiceBindingAsyncBindingRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       "test invalid state",
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncBindRetryDurationExceeded(t, updatedBinding, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorAsyncOpTimeoutReason + " " + "The asynchronous Bind operation timed out and will not be retried",
				corev1.EventTypeWarning + " " + errorReconciliationRetryTimeoutReason + " " + "Stopping reconciliation retries because too much time has elapsed",
				corev1.EventTypeWarning + " " + errorServiceBindingOrphanMitigation + " " + "Starting orphan mitigation",
			},
		},
		{
			name:    "bind - operation succeeded but GET failed",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateSucceeded,
					Description: strPtr(lastOperationDescription),
				},
			},
			getBindingReaction: &fakeosb.GetBindingReaction{
				Error: fmt.Errorf("some error"),
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAndGetBindingActions,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncBindErrorAfterStateSucceeded(t, updatedBinding, errorFetchingBindingFailedReason, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorFetchingBindingFailedReason + " " + "Could not do a GET on binding resource: some error",
				corev1.EventTypeWarning + " " + errorFetchingBindingFailedReason + " " + "Could not do a GET on binding resource: some error",
				corev1.EventTypeWarning + " " + errorServiceBindingOrphanMitigation + " " + "Starting orphan mitigation",
			},
		},
		{
			name:    "bind - operation succeeded but binding injection failed",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateSucceeded,
					Description: strPtr(lastOperationDescription),
				},
			},
			getBindingReaction: &fakeosb.GetBindingReaction{
				Response: &osb.GetBindingResponse{
					Credentials: map[string]interface{}{
						"a": "b",
						"c": "d",
					},
				},
			},
			environmentSetupFunc: func(t *testing.T, fakeKubeClient *clientgofake.Clientset, sharedInformers v1beta1informers.Interface) {
				sharedInformers.ServiceBrokers().Informer().GetStore().Add(getTestServiceBroker())
				sharedInformers.ServiceClasses().Informer().GetStore().Add(getTestBindingRetrievableServiceClass())
				sharedInformers.ServicePlans().Informer().GetStore().Add(getTestServicePlan())
				sharedInformers.ServiceInstances().Informer().GetStore().Add(getTestServiceInstanceWithNamespacedRefsAndStatus(v1beta1.ConditionTrue))

				addGetNamespaceReaction(fakeKubeClient)
				addGetSecretReaction(fakeKubeClient, &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServiceBindingName, Namespace: testNamespace},
				})
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAndGetBindingActions,
			validateKubeActionsFunc: func(t *testing.T, actions []clientgotesting.Action) {
				assertNumberOfActions(t, actions, 1)
				assertActionEquals(t, actions[0], "get", "secrets")
			},
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncBindErrorAfterStateSucceeded(t, updatedBinding, errorInjectingBindResultReason, originalBinding)
			},
			shouldFinishPolling: true, // should not be requeued in polling queue; will drop back to default rate limiting
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorInjectingBindResultReason + " " + `Error injecting bind results: Secret "test-ns/test-binding" is not owned by ServiceBinding, controllerRef: nil`,
				corev1.EventTypeWarning + " " + errorInjectingBindResultReason + " " + `Error injecting bind results: Secret "test-ns/test-binding" is not owned by ServiceBinding, controllerRef: nil`,
				corev1.EventTypeWarning + " " + errorServiceBindingOrphanMitigation + " " + "Starting orphan mitigation",
			},
		},
		{
			name:    "bind - succeeded",
			binding: getTestServiceBindingAsyncBinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateSucceeded,
					Description: strPtr(lastOperationDescription),
				},
			},
			getBindingReaction: &fakeosb.GetBindingReaction{
				Response: &osb.GetBindingResponse{
					Credentials: map[string]interface{}{
						"a": "b",
						"c": "d",
					},
				},
			},
			environmentSetupFunc: func(t *testing.T, fakeKubeClient *clientgofake.Clientset, sharedInformers v1beta1informers.Interface) {
				sharedInformers.ServiceBrokers().Informer().GetStore().Add(getTestServiceBroker())
				sharedInformers.ServiceClasses().Informer().GetStore().Add(getTestBindingRetrievableServiceClass())
				sharedInformers.ServicePlans().Informer().GetStore().Add(getTestServicePlan())
				sharedInformers.ServiceInstances().Informer().GetStore().Add(getTestServiceInstanceWithNamespacedRefsAndStatus(v1beta1.ConditionTrue))

				addGetNamespaceReaction(fakeKubeClient)
				addGetSecretNotFoundReaction(fakeKubeClient)
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAndGetBindingActions,
			validateKubeActionsFunc: func(t *testing.T, actions []clientgotesting.Action) {
				assertNumberOfActions(t, actions, 2)
				assertActionEquals(t, actions[0], "get", "secrets")
				assertActionEquals(t, actions[1], "create", "secrets")
			},
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingOperationSuccess(t, updatedBinding, v1beta1.ServiceBindingOperationBind, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + successInjectedBindResultReason + " " + successInjectedBindResultMessage},
		},
		// Unbind as part of deletion
		{
			name:    "unbind - succeeded",
			binding: getTestServiceBindingAsyncUnbinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateSucceeded,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 2)
				assertUpdateStatus(t, actions[0], originalBinding)
				updatedBinding := assertUpdate(t, actions[1], originalBinding)

				assertServiceBindingOperationSuccess(t, updatedBinding, v1beta1.ServiceBindingOperationUnbind, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + successUnboundReason + " " + "The binding was deleted successfully"},
		},
		{
			name:    "unbind - 410 Gone considered succeeded",
			binding: getTestServiceBindingAsyncUnbinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Error: osb.HTTPStatusCodeError{
					StatusCode: http.StatusGone,
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 2)
				assertUpdateStatus(t, actions[0], originalBinding)
				updatedBinding := assertUpdate(t, actions[1], originalBinding)

				assertServiceBindingOperationSuccess(t, updatedBinding, v1beta1.ServiceBindingOperationUnbind, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + successUnboundReason + " " + "The binding was deleted successfully"},
		},
		{
			name:    "unbind - in progress",
			binding: getTestServiceBindingAsyncUnbinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateInProgress,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncInProgress(t, updatedBinding, v1beta1.ServiceBindingOperationUnbind, asyncUnbindingReason, testOperation, originalBinding)
			},
			shouldFinishPolling: false,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + asyncUnbindingReason + " " + "The binding is being deleted asynchronously (testdescr)"},
		},
		{
			name:    "unbind - error",
			binding: getTestServiceBindingAsyncUnbinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Error: fmt.Errorf("random error"),
			},
			validateBrokerActionsFunc:  validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: nil, // does not update resources
			shouldFinishPolling:        false,
			expectedEvents:             []string{corev1.EventTypeWarning + " " + errorPollingLastOperationReason + " " + "Error polling last operation: random error"},
		},
		{
			name:    "unbind - failed (retries)",
			binding: getTestServiceBindingAsyncUnbinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateFailed,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingRequestRetriableError(
					t,
					updatedBinding,
					v1beta1.ServiceBindingOperationUnbind,
					errorUnbindCallReason,
					originalBinding,
				)
			},
			shouldError:         true,
			shouldFinishPolling: true,
			expectedEvents:      []string{corev1.EventTypeWarning + " " + errorUnbindCallReason + " " + "Unbind call failed: " + lastOperationDescription},
		},
		{
			name:    "unbind - invalid state",
			binding: getTestServiceBindingAsyncUnbinding(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       "test invalid state",
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc:  validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: nil, // does not update resources
			shouldFinishPolling:        false,
			expectedEvents:             []string{}, // does not record event
		},
		{
			name:    "unbind - in progress - retry duration exceeded",
			binding: getTestServiceBindingAsyncUnbindingRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateInProgress,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncUnbindRetryDurationExceeded(
					t,
					updatedBinding,
					v1beta1.ServiceBindingOperationUnbind,
					errorAsyncOpTimeoutReason,
					errorReconciliationRetryTimeoutReason,
					originalBinding,
				)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorAsyncOpTimeoutReason + " " + "The asynchronous Unbind operation timed out and will not be retried",
				corev1.EventTypeWarning + " " + errorReconciliationRetryTimeoutReason + " " + "Stopping reconciliation retries because too much time has elapsed",
			},
		},
		{
			name:    "unbind - invalid state - retry duration exceeded",
			binding: getTestServiceBindingAsyncUnbindingRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       "test invalid state",
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncUnbindRetryDurationExceeded(
					t,
					updatedBinding,
					v1beta1.ServiceBindingOperationUnbind,
					errorAsyncOpTimeoutReason,
					errorReconciliationRetryTimeoutReason,
					originalBinding,
				)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorAsyncOpTimeoutReason + " " + "The asynchronous Unbind operation timed out and will not be retried",
				corev1.EventTypeWarning + " " + errorReconciliationRetryTimeoutReason + " " + "Stopping reconciliation retries because too much time has elapsed",
			},
		},
		{
			name:    "unbind - failed - retry duration exceeded",
			binding: getTestServiceBindingAsyncUnbindingRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateFailed,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingRequestFailingError(
					t,
					updatedBinding,
					v1beta1.ServiceBindingOperationUnbind,
					errorUnbindCallReason,
					errorReconciliationRetryTimeoutReason,
					originalBinding,
				)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorUnbindCallReason + " " + "Unbind call failed: " + lastOperationDescription,
				corev1.EventTypeWarning + " " + errorReconciliationRetryTimeoutReason + " " + "Stopping reconciliation retries because too much time has elapsed",
			},
		},
		// Unbind as part of orphan mitigation
		{
			name:    "orphan mitigation - succeeded",
			binding: getTestServiceBindingAsyncOrphanMitigation(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateSucceeded,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingOrphanMitigationSuccess(t, updatedBinding, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + successOrphanMitigationReason + " " + successOrphanMitigationMessage},
		},
		{
			name:    "orphan mitigation - 410 Gone considered succeeded",
			binding: getTestServiceBindingAsyncOrphanMitigation(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Error: osb.HTTPStatusCodeError{
					StatusCode: http.StatusGone,
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingOrphanMitigationSuccess(t, updatedBinding, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + successOrphanMitigationReason + " " + successOrphanMitigationMessage},
		},
		{
			name:    "orphan mitigation - in progress",
			binding: getTestServiceBindingAsyncOrphanMitigation(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateInProgress,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncInProgress(t, updatedBinding, v1beta1.ServiceBindingOperationBind, asyncUnbindingReason, testOperation, originalBinding)
			},
			shouldFinishPolling: false,
			expectedEvents:      []string{corev1.EventTypeNormal + " " + asyncUnbindingReason + " " + "The binding is being deleted asynchronously (testdescr)"},
		},
		{
			name:    "orphan mitigation - error",
			binding: getTestServiceBindingAsyncOrphanMitigation(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Error: fmt.Errorf("random error"),
			},
			validateBrokerActionsFunc:  validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: nil, // does not update resources
			shouldFinishPolling:        false,
			expectedEvents:             []string{corev1.EventTypeWarning + " " + errorPollingLastOperationReason + " " + "Error polling last operation: random error"},
		},
		{
			name:    "orphan mitigation - failed (retries)",
			binding: getTestServiceBindingAsyncOrphanMitigation(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateFailed,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingRequestRetriableOrphanMitigation(t, updatedBinding, errorUnbindCallReason, originalBinding)
			},
			shouldError:         true,
			shouldFinishPolling: true,
			expectedEvents:      []string{corev1.EventTypeWarning + " " + errorUnbindCallReason + " " + "Unbind call failed: " + lastOperationDescription},
		},
		{
			name:    "orphan mitigation - invalid state",
			binding: getTestServiceBindingAsyncOrphanMitigation(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       "test invalid state",
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc:  validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: nil, // does not update resources
			shouldFinishPolling:        false,
			expectedEvents:             []string{}, // does not record event
		},
		{
			name:    "orphan mitigation - in progress - retry duration exceeded",
			binding: getTestServiceBindingAsyncOrphanMitigationRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateInProgress,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncOrphanMitigationRetryDurationExceeded(t, updatedBinding, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorAsyncOpTimeoutReason + " " + "The asynchronous Unbind operation timed out and will not be retried",
				corev1.EventTypeWarning + " " + errorOrphanMitigationFailedReason + " " + "Orphan mitigation failed: Stopping reconciliation retries because too much time has elapsed",
			},
		},
		{
			name:    "orphan mitigation - invalid state - retry duration exceeded",
			binding: getTestServiceBindingAsyncOrphanMitigationRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       "test invalid state",
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncOrphanMitigationRetryDurationExceeded(t, updatedBinding, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorAsyncOpTimeoutReason + " " + "The asynchronous Unbind operation timed out and will not be retried",
				corev1.EventTypeWarning + " " + errorOrphanMitigationFailedReason + " " + "Orphan mitigation failed: Stopping reconciliation retries because too much time has elapsed",
			},
		},
		{
			name:    "orphan mitigation - failed - retry duration exceeded",
			binding: getTestServiceBindingAsyncOrphanMitigationRetryDurationExceeded(testOperation),
			pollReaction: &fakeosb.PollBindingLastOperationReaction{
				Response: &osb.LastOperationResponse{
					State:       osb.StateFailed,
					Description: strPtr(lastOperationDescription),
				},
			},
			validateBrokerActionsFunc: validatePollBindingLastOperationAction,
			assertPerfomerdActionsFunc: func(t *testing.T, actions []clientgotesting.Action, originalBinding *v1beta1.ServiceBinding) {
				assertNumberOfActions(t, actions, 1)
				updatedBinding := assertUpdateStatus(t, actions[0], originalBinding)

				assertServiceBindingAsyncOrphanMitigationRetryDurationExceeded(t, updatedBinding, originalBinding)
			},
			shouldFinishPolling: true,
			expectedEvents: []string{
				corev1.EventTypeWarning + " " + errorUnbindCallReason + " " + "Unbind call failed: " + lastOperationDescription,
				corev1.EventTypeWarning + " " + errorOrphanMitigationFailedReason + " " + "Orphan mitigation failed: Stopping reconciliation retries because too much time has elapsed",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakeKubeClient, fakeCatalogClient, fakeServiceBrokerClient, testController, sharedInformers := newTestController(t, fakeosb.FakeClientConfiguration{
				PollBindingLastOperationReaction: tc.pollReaction,
				GetBindingReaction:               tc.getBindingReaction,
			})

			if tc.environmentSetupFunc != nil {
				tc.environmentSetupFunc(t, fakeKubeClient, sharedInformers)
			} else {
				// default
				sharedInformers.ServiceBrokers().Informer().GetStore().Add(getTestServiceBroker())
				sharedInformers.ServiceClasses().Informer().GetStore().Add(getTestBindingRetrievableServiceClass())
				sharedInformers.ServicePlans().Informer().GetStore().Add(getTestServicePlan())
				sharedInformers.ServiceInstances().Informer().GetStore().Add(getTestServiceInstanceWithNamespacedRefsAndStatus(v1beta1.ConditionTrue))
			}

			bindingKey := tc.binding.Namespace + "/" + tc.binding.Name

			err := testController.pollServiceBinding(tc.binding)
			if tc.shouldError && err == nil {
				t.Fatalf("expected error when polling service binding but there was none")
			} else if !tc.shouldError && err != nil {
				t.Fatalf("unexpected error when polling service binding: %v", err)
			}

			if tc.shouldFinishPolling && testController.bindingPollingQueue.NumRequeues(bindingKey) != 0 {
				t.Fatalf("Expected polling queue to not have any record of test binding as polling should have completed")
			} else if !tc.shouldFinishPolling && testController.bindingPollingQueue.NumRequeues(bindingKey) != 1 {
				t.Fatalf("Expected polling queue to have record of seeing test binding once")
			}

			// Broker actions
			brokerActions := fakeServiceBrokerClient.Actions()

			if tc.validateBrokerActionsFunc != nil {
				tc.validateBrokerActionsFunc(t, brokerActions)
			} else {
				assertNumberOfBrokerActions(t, brokerActions, 0)
			}

			// Kube actions
			kubeActions := fakeKubeClient.Actions()

			if tc.validateKubeActionsFunc != nil {
				tc.validateKubeActionsFunc(t, kubeActions)
			} else {
				assertNumberOfActions(t, kubeActions, 0)
			}

			// Catalog actions
			actions := fakeCatalogClient.Actions()
			if tc.assertPerfomerdActionsFunc != nil {
				tc.assertPerfomerdActionsFunc(t, actions, tc.binding)
			} else {
				assertNumberOfActions(t, actions, 0)
			}

			// Events
			events := getRecordedEvents(testController)
			assertNumEvents(t, events, len(tc.expectedEvents))

			for idx, expectedEvent := range tc.expectedEvents {
				if e, a := expectedEvent, events[idx]; e != a {
					t.Fatalf("Received unexpected event #%v, expected %v got %v", idx, e, a)
				}
			}
		})
	}
}

// TestReconcileServiceBindingAsynchronousUnbindNamespacedRefs tests the situation where the
// controller receives an asynchronous bind response back from the broker when
// doing an unbind call.
func TestReconcileServiceBindingAsynchronousUnbindNamespacedRefs(t *testing.T) {
	err := utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.NamespacedServiceBroker))
	if err != nil {
		t.Fatalf("Could not enable NamespacedServiceBroker feature flag.")
	}
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.NamespacedServiceBroker))

	utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=true", scfeatures.AsyncBindingOperations))
	defer utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%v=false", scfeatures.AsyncBindingOperations))

	key := osb.OperationKey(testOperation)
	fakeKubeClient, fakeCatalogClient, fakeServiceBrokerClient, testController, sharedInformers := newTestController(t, fakeosb.FakeClientConfiguration{
		UnbindReaction: &fakeosb.UnbindReaction{
			Response: &osb.UnbindResponse{
				Async:        true,
				OperationKey: &key,
			},
		},
	})

	sharedInformers.ServiceBrokers().Informer().GetStore().Add(getTestServiceBroker())
	sharedInformers.ServiceClasses().Informer().GetStore().Add(getTestBindingRetrievableServiceClass())
	sharedInformers.ServicePlans().Informer().GetStore().Add(getTestServicePlan())
	sharedInformers.ServiceInstances().Informer().GetStore().Add(getTestServiceInstanceWithNamespacedRefsAndStatus(v1beta1.ConditionTrue))

	binding := getTestServiceBindingUnbinding()
	bindingKey := binding.Namespace + "/" + binding.Name

	fakeCatalogClient.AddReactor("get", "servicebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, binding, nil
	})

	if testController.bindingPollingQueue.NumRequeues(bindingKey) != 0 {
		t.Fatalf("Expected polling queue to not have any record of test binding")
	}

	if err := reconcileServiceBinding(t, testController, binding); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	binding = assertServiceBindingUnbindInProgressIsTheOnlyCatalogAction(t, fakeCatalogClient, binding)
	fakeCatalogClient.ClearActions()

	assertDeleteSecretAction(t, fakeKubeClient.Actions(), binding.Spec.SecretName)
	fakeKubeClient.ClearActions()

	assertNumberOfBrokerActions(t, fakeServiceBrokerClient.Actions(), 0)

	if err := reconcileServiceBinding(t, testController, binding); err != nil {
		t.Fatalf("a valid binding should not fail: %v", err)
	}

	if testController.bindingPollingQueue.NumRequeues(bindingKey) != 1 {
		t.Fatalf("Expected polling queue to have a record of seeing test binding once")
	}

	// Broker actions
	brokerActions := fakeServiceBrokerClient.Actions()
	assertNumberOfBrokerActions(t, brokerActions, 1)
	assertUnbind(t, brokerActions[0], &osb.UnbindRequest{
		BindingID:         testServiceBindingGUID,
		InstanceID:        testServiceInstanceGUID,
		ServiceID:         testServiceClassGUID,
		PlanID:            testServicePlanGUID,
		AcceptsIncomplete: true,
	})

	// Kube actions
	assertDeleteSecretAction(t, fakeKubeClient.Actions(), binding.Spec.SecretName)

	// Service Catalog actions
	actions := fakeCatalogClient.Actions()
	assertNumberOfActions(t, actions, 1)

	updatedServiceBinding := assertUpdateStatus(t, actions[0], binding).(*v1beta1.ServiceBinding)
	assertServiceBindingAsyncInProgress(t, updatedServiceBinding, v1beta1.ServiceBindingOperationUnbind, asyncUnbindingReason, testOperation, binding)

	// Events
	events := getRecordedEvents(testController)
	assertNumEvents(t, events, 1)

	expectedEvent := corev1.EventTypeNormal + " " + asyncUnbindingReason + " " + asyncUnbindingMessage
	if e, a := expectedEvent, events[0]; e != a {
		t.Fatalf("Received unexpected event, expected %v got %v", e, a)
	}
}
